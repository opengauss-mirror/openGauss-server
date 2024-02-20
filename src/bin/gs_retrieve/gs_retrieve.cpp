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
 * gs_retrieve.cpp
 *      help retrieve data when asynchronous standby is promoted.
 *
 * IDENTIFICATION
 *    src/bin/gs_retrieve/gs_retrieve.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "gs_retrieve.h"
#include "getopt_long.h"
#include "access/xlog_internal.h"
#include "access/xact.h"
#include "bin/elog.h"
#include "utils/pg_crc.h"
#include "tool_common.h"
#include "libpq/libpq-fe.h"
#include "storage/dss/dss_adaptor.h"
#include "storage/file/fio_device.h"

static void do_advice(void)
{
    write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
}

static void do_help(void)
{
    write_stderr(_("%s is a utility to retrieve data from old master when asynchronous standby is promoted.\n\n"),
        progname);
    write_stderr(_("Usage:\n"));
    write_stderr(_("  %s [OPTION]...\n"), progname);
    write_stderr(_("\nOptions:\n"));
    write_stderr(_("  --newhost=HOSTNAME           new master host\n"));
    write_stderr(_("  --newport=PORT               new master port number\n"));
    write_stderr(_("  --oldhost=HOSTNAME           old master host\n"));
    write_stderr(_("  --oldport=PORT               old master port number\n"));
    write_stderr(_("  --force                      force use non-historic-snapshot way to logical decoding\n"));
    write_stderr(_("  -U, --username=NAME          connect as specified database user\n"));
    write_stderr(_("  -W, --password               force password prompt (should happen automatically)\n"));
    write_stderr(_("  -d, --dbname=DBNAME          database to connect to\n"));
    write_stderr(_("  -P, --plugin=PLUGIN          use output plugin PLUGIN (defaults to mppdb_decoding)\n"));
    write_stderr(_("  -?, --help                   show this help, then exit\n"));
    write_stderr(_("  -f, --file=FILE              receive log into this file. - for stdout\n"));
    write_stderr(_("\n"));
}

PGconn* get_remote_connection()
{
    char repl_conninfo_str[MAXPGPATH] = {0};
    int rc = 0;

    rc = snprintf_s(repl_conninfo_str, sizeof(repl_conninfo_str), sizeof(repl_conninfo_str) -1,
        "localhost=%s localport=%d host=%s port=%d "
        "dbname=postgres replication=hadr_main_standby "
        "fallback_application_name=gs_retrieve "
        "connect_timeout=%d rw_timeout=%d "
        "options='-c remotetype=application'",
        oldHost, oldPort, newHost, newPort, connectTimeout, recvTimeout);
    securec_check_ss_c(rc, "", "");

    PGconn* conn = PQconnectdb(repl_conninfo_str);
    if (conn == NULL) {
        write_stderr(_("%s: connection failed cause get connection is null.\n"), progname);
        DISCONNECT_AND_RETURN_NULL(conn);
    }

    if (PQstatus(conn) != CONNECTION_OK) {
        write_stderr(_("%s: connection to %s failed cause %s.\n"), progname, newHost, PQerrorMessage(conn));
        DISCONNECT_AND_RETURN_NULL(conn);
    }

    return conn;
}

PGconn* get_local_connection()
{
    char repl_conninfo_str[MAXPGPATH] = {0};
    int rc = 0;
    int retryCount = 0;
    const int retryMaxTimes = 3;
    const int PASSWDLEN = 100;
    PGconn* conn = NULL;
    char *userPasswd = NULL;

retry:
    if (++retryCount > retryMaxTimes) {
        DISCONNECT_AND_RETURN_NULL(conn);
    }

    if (dbgetpassword == 1) {
        userPasswd = simple_prompt(_("Password: "), PASSWDLEN, false);
    }

    rc = snprintf_s(repl_conninfo_str, sizeof(repl_conninfo_str), sizeof(repl_conninfo_str) -1,
        "dbname=%s port=%d host='%s' "
        "user='%s' password='%s' "
        "application_name=gs_retrieve "
        "connect_timeout=%d rw_timeout=%d "
        "options='-c xc_maintenance_mode=on -c remotetype=internaltool'",
        dbname, oldPort, oldHost, userName, userPasswd, connectTimeout, recvTimeout);
    securec_check_ss_c(rc, "", "");

    if (userPasswd != NULL) {
        rc = memset_s(userPasswd, strlen(userPasswd), 0, strlen(userPasswd));
        securec_check_c(rc, "", "");
    }

    conn = PQconnectdb(repl_conninfo_str);
    rc = memset_s(repl_conninfo_str, strlen(repl_conninfo_str), 0, strlen(repl_conninfo_str));
    securec_check_c(rc, "", "");
    if (conn == NULL) {
        write_stderr(_("%s: connection failed cause get connection is null.\n"), progname);
        DISCONNECT_AND_RETURN_NULL(conn);
    }

    if (PQstatus(conn) == CONNECTION_BAD && (strstr(PQerrorMessage(conn), "password") != NULL)) {
        write_stderr(_("%s: connection to %s failed cause %s"), progname, oldHost, PQerrorMessage(conn));
        PQfinish(conn);
        conn = NULL;
        goto retry;
    }

    if (PQstatus(conn) != CONNECTION_OK) {
        write_stderr(_("%s: connection to %s failed cause %s"), progname, oldHost, PQerrorMessage(conn));
        DISCONNECT_AND_RETURN_NULL(conn);
    }

    return conn;
}

bool check_remote_common_lsn(PGconn* conn, XLogRecPtr recptr, pg_crc32 standby_reccrc)
{
    char cmd[MAXPGPATH] = {0};
    int rc = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "IDENTIFY_CONSISTENCE %X/%X",
        (uint32)(recptr >> BITS_PER_INT), (uint32)recptr);
    securec_check_ss_c(rc, "\0", "\0");

    PGresult *res = PQexec(conn, cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        write_stderr(_("%s: could not IDENTIFY_CONSISTENCE system: %s\n"), progname, PQerrorMessage(conn));
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }

    char* primary_reccrc = PQgetvalue(res, 0, 0);
    pg_crc32 reccrc = 0;

    if (primary_reccrc && sscanf_s(primary_reccrc, "%8X", &reccrc) != 1) {
        write_stderr(_("%s: could not parse remote crc: %s\n"), progname, primary_reccrc);
        PQclear(res);
        PQfinish(conn);
        exit(1);
    }
    PQclear(res);

    if (reccrc == standby_reccrc) {
        return true;
    }
    return false;
}

XLogRecord* retrieve_xlog_read_record(XLogReaderState* xlogreader, XLogRecPtr *searchptr, char **errormsg)
{
    if (ss_instance_config.dss.enable_dss) {
        return XLogReadRecordFromAllDir(xlogDirs, xlogDirNum, xlogreader, *searchptr, errormsg);
    } else {
        return XLogReadRecord(xlogreader, *searchptr, errormsg);
    }
}


/*
 * Search xlog from the max lsn to find the divergence of new/old hosts and
 * collect the to-be-decoded xids.
 * By the way, try to find the min begin lsn of to-be-decoded xids. Maybe it
 * would be not the actual min lsn.
 */
XLogRecPtr get_diverge_lsn_and_need_decoded_xids(XLogReaderState* xlogreader, XLogRecPtr *searchptr,
    XLogRecPtr *needDecodedMinLsn)
{
    PGconn* conn = get_remote_connection();
    if (conn == NULL) {
        return InvalidXLogRecPtr;
    }

    XLogRecord* record = NULL;
    char* errormsg = NULL;
    while (!XLogRecPtrIsInvalid(*searchptr)) {
        record = retrieve_xlog_read_record(xlogreader, searchptr, &errormsg);
        if (record == NULL) {
            if (errormsg != NULL) {
                write_stderr(_("%s: could not find previous WAL record at %X/%X: %s\n"),
                    progname, (uint32)(*searchptr >> BITS_PER_INT), (uint32)*searchptr, errormsg);
            } else {
                write_stderr(_("%s: could not find previous WAL record at %X/%X\n"),
                    progname, (uint32)(*searchptr >> BITS_PER_INT), (uint32)*searchptr);
            }
            CloseXlogFile();
            PQfinish(conn);
            return InvalidXLogRecPtr;
        }

        uint8 xact_info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
        /*
         * Check the lsn whether exist or not on new host.
         * To reduce queries, we only check commit-xlog, because others are not the key,
         * we just need to know which committed-transactions are not synchronized to new
         * host.
         */
        if (IS_COMMIT_XLOG(record, xact_info)) {
            if (check_remote_common_lsn(conn, xlogreader->ReadRecPtr, record->xl_crc)) {
                write_log(_("%s: find diverge lsn %X/%X\n"),
                    progname, (uint32)(xlogreader->ReadRecPtr >> BITS_PER_INT), (uint32)xlogreader->ReadRecPtr);
                break;
            }

            /*
             * Commit-xlog is not exist on new host, it means that this transaction is not
             * synchronized to new host, add it into needDecodedXids.
             */
            needDecodedXids.insert(record->xl_xid);
        } else if (record->xl_xid != InvalidTransactionId &&
                   needDecodedXids.find(record->xl_xid) != needDecodedXids.end()) {
            /* if the xlog belong to one transaction of needDecodedXids, set needDecodedMinLsn */
            *needDecodedMinLsn = xlogreader->ReadRecPtr;
        }

        *searchptr = record->xl_prev;
    }
    PQfinish(conn);

    return xlogreader->EndRecPtr;
}

static bool check_need_decode_xids_all_not_in_rx(xl_running_xacts* xlrec)
{
    bool found = false;
    for (int i = 0; i < xlrec->xcnt; i++) {
        if (needDecodedXids.find(xlrec->xids[i]) != needDecodedXids.end()) {
            found = true;
            break;
        }
    }
    return found;
}

/*
 * Return the actual min begin-lsn of needDecodedXids.
 */
void get_need_decoded_min_lsn(XLogReaderState* xlogreader, XLogRecPtr *searchptr, XLogRecPtr *needDecodedMinLsn)
{
    XLogRecord* record = NULL;
    char* errormsg = NULL;
    while (!XLogRecPtrIsInvalid(*searchptr)) {
        record = retrieve_xlog_read_record(xlogreader, searchptr, &errormsg);
        if (record == NULL) {
            if (errormsg != NULL) {
                write_stderr(_("%s: could not find previous WAL record at %X/%X: %s\n"),
                    progname, (uint32)(*searchptr >> BITS_PER_INT), (uint32)*searchptr, errormsg);
            } else {
                write_stderr(_("%s: could not find previous WAL record at %X/%X\n"),
                    progname, (uint32)(*searchptr >> BITS_PER_INT), (uint32)*searchptr);
            }
            return;
        }

        if (record->xl_xid != InvalidTransactionId && needDecodedXids.find(record->xl_xid) != needDecodedXids.end()) {
            *needDecodedMinLsn = xlogreader->ReadRecPtr;
        } else if ((record->xl_rmid == RM_STANDBY_ID) && (record->xl_info & (~XLR_INFO_MASK)) == XLOG_RUNNING_XACTS) {
            /*
             * If all needDecodedXids are not in the running_xacts, we can stop searching
             * right now. Because it means that it is impossible to begin before needDecodedMinLsn.
             */
            xl_running_xacts* xlrec = (xl_running_xacts*)XLogRecGetData(xlogreader);
            if (!check_need_decode_xids_all_not_in_rx(xlrec)) {
                break;
            }
        }
        *searchptr = record->xl_prev;
    }
}

static bool check_before_committed_xids_all_in_rx(std::set<TransactionId> beforeCommittedXids, xl_running_xacts* xlrec)
{
    bool allFound = true;
    for (int i = 0; i < xlrec->xcnt; i++) {
        if (beforeCommittedXids.find(xlrec->xids[i]) == beforeCommittedXids.end()) {
            allFound = false;
            break;
        }
    }
    return allFound;
}

/*
 * Searching xlog from diverge lsn, to find a XLOG_RUNNING_XACTS xlog that running
 * transactions in it are all committed before needDecodedMinLsn.
 */
XLogRecPtr get_restart_lsn(XLogReaderState* xlogreader, XLogRecPtr *searchptr)
{
    std::set<TransactionId> beforeCommittedXids;
    XLogRecord* record = NULL;
    char* errormsg = NULL;
    while (!XLogRecPtrIsInvalid(*searchptr)) {
        record = retrieve_xlog_read_record(xlogreader, searchptr, &errormsg);
        if (record == NULL) {
            if (errormsg != NULL) {
                write_stderr(_("%s: could not find previous WAL record at %X/%X: %s\n"),
                    progname, (uint32)(*searchptr >> BITS_PER_INT), (uint32)*searchptr, errormsg);
            } else {
                write_stderr(_("%s: could not find previous WAL record at %X/%X\n"),
                    progname, (uint32)(*searchptr >> BITS_PER_INT), (uint32)*searchptr);
            }
            return InvalidXLogRecPtr;
        }

        uint8 xact_info = XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK;
        if (IS_COMMIT_XLOG(record, xact_info)) {
            /* record committed xacts. */
            beforeCommittedXids.insert(record->xl_xid);
        } else if ((record->xl_rmid == RM_STANDBY_ID) && (record->xl_info & (~XLR_INFO_MASK)) == XLOG_RUNNING_XACTS) {
            /* check whether running xacts all committed before needDecodedMinLsn or not */
            xl_running_xacts* xlrec = (xl_running_xacts*)XLogRecGetData(xlogreader);
            if (check_before_committed_xids_all_in_rx(beforeCommittedXids, xlrec)) {
                break;
            }
        }
        *searchptr = record->xl_prev;
    }

    return xlogreader->ReadRecPtr;
}

bool print_col_name(PGresult *result, FILE *decodeFile, char *colDel, char newLine)
{
    int maxColumns = PQnfields(result);
    for (int col = 0; col < maxColumns; col++) {
        char *colName = PQfname(result, col);
        if (strlen(colName) != 0 && fwrite(colName, strlen(colName), 1, decodeFile) == 0) {
            write_stderr(_("%s: fwrite %s failed\n"), progname, outputFile);
            return false;
        }

        if (col < maxColumns - 1 && fwrite(colDel, strlen(colDel), 1, decodeFile) == 0) {
            write_stderr(_("%s: fwrite %s failed\n"), progname, outputFile);
            return false;
        }
    }
    if (fwrite(&newLine, sizeof(newLine), 1, decodeFile) == 0) {
        write_stderr(_("%s: fwrite %s failed\n"), progname, outputFile);
        return false;
    }
    return true;
}

bool print_row_value(PGresult *result, FILE *decodeFile, char *colDel, char newLine)
{
    int maxRows = PQntuples(result);
    int maxColumns = PQnfields(result);
    for (int row = 0; row < maxRows; row++) {
        char *endPtr = NULL;
        char *strXid = PQgetvalue(result, row, 1);
        TransactionId xid = (TransactionId)strtoul(strXid, &endPtr, 10);
        if (endPtr == strXid || errno == ERANGE) {
            continue;
        }

        if (doAreaChange && needDecodedXids.find(xid) == needDecodedXids.end()) {
            continue;
        }

        for (int col = 0; col < maxColumns; col++) {
            char *val = PQgetvalue(result, row, col);
            if (strlen(val) != 0 && fwrite(val, strlen(val), 1, decodeFile) == 0) {
                write_stderr(_("%s: fwrite %s failed\n"), progname, outputFile);
                return false;
            }

            if (col < maxColumns - 1 && fwrite(colDel, strlen(colDel), 1, decodeFile) == 0) {
                write_stderr(_("%s: fwrite %s failed\n"), progname, outputFile);
                return false;
            }
        }

        if (fwrite(&newLine, sizeof(newLine), 1, decodeFile) == 0) {
            write_stderr(_("%s: fwrite %s failed\n"), progname, outputFile);
            return false;
        }
    }
    return true;
}

bool parse_print_decode_result(PGresult *result)
{
    static bool alreadyPrintColName = false;
    char colDel[] = "   |   ";
    char newLine = '\n';
    FILE *decodeFile = NULL;
    if (strcmp(outputFile, "-") == 0) {
        decodeFile = stdout;
    } else if ((decodeFile = fopen(outputFile, "a+")) == NULL) {
        write_stderr(_("%s: fopen %s failed\n"), progname, outputFile);
        return false;
    }

    /* print column */
    if (!alreadyPrintColName) {
        if (!print_col_name(result, decodeFile, colDel, newLine)) {
            FCLOSE_AND_RETURN_FALSE(decodeFile);
        }
        alreadyPrintColName = true;
    }

    /* print row */
    if (!print_row_value(result, decodeFile, colDel, newLine)) {
        FCLOSE_AND_RETURN_FALSE(decodeFile);
    }

    if (strcmp(outputFile, "-") != 0) {
        (void)fclose(decodeFile);
    }
    return true;
}

void logical_replication_for_retrieve(PGconn *conn, XLogRecPtr restartLsn, XLogRecPtr confirmedFlush)
{
    const int upto_nchanges = 1000;   /* 1000 statement per query */
    char sql_cmd[MAXCMDLEN] = {0};
    int rc = snprintf_s(sql_cmd, MAXCMDLEN, MAXCMDLEN - 1,
        "select * from pg_create_logical_replication_slot('gs_retrieve_slot', '%s', '%X/%X', '%X/%X');",
        decodePlugin, (uint32)(restartLsn >> BITS_PER_INT), (uint32)restartLsn,
        (uint32)(confirmedFlush >> BITS_PER_INT), (uint32)confirmedFlush);
    securec_check_ss_c(rc, "", "");

    PGresult *result = PQexec(conn, sql_cmd);
    if (result == NULL || PQresultStatus(result) != PGRES_TUPLES_OK) {
        write_stderr(_("%s: create_logical_replication_slot failed\n"), progname);
        PQclear(result);
        PQfinish(conn);
        exit(1);
    }

    PQclear(result);

    /* Decode up to 1000 entries at a time to avoid taking up too much memory. */
    rc = snprintf_s(sql_cmd, MAXCMDLEN, MAXCMDLEN - 1,
        "select * from pg_logical_slot_get_changes('gs_retrieve_slot', NULL, %d);", upto_nchanges);
    securec_check_ss_c(rc, "", "");

    while (true) {
        result = PQexec(conn, sql_cmd);
        if (result == NULL || PQresultStatus(result) != PGRES_TUPLES_OK) {
            write_stderr(_("%s: pg_logical_slot_get_changes failed: \n"), progname);
            PQclear(result);
            PQfinish(conn);
            exit(1);
        }
        if (PQntuples(result) == 0) {
            break;
        }
        if (!parse_print_decode_result(result)) {
            PQclear(result);
            PQfinish(conn);
            exit(1);
        }
        PQclear(result);
    }

    PQclear(result);

    rc = snprintf_s(sql_cmd, MAXCMDLEN, MAXCMDLEN - 1,
        "select * from pg_drop_replication_slot('gs_retrieve_slot');");
    securec_check_ss_c(rc, "", "");

    result = PQexec(conn, sql_cmd);
    if (result == NULL || PQresultStatus(result) != PGRES_TUPLES_OK) {
        write_stderr(_("%s: pg_drop_replication_slot failed\n"), progname);
        PQclear(result);
        PQfinish(conn);
        exit(1);
    }
    PQclear(result);
}

void logical_replication_for_area_decode(PGconn *conn, XLogRecPtr startLsn, XLogRecPtr endLsn)
{
    char sql_cmd[MAXCMDLEN] = {0};
    int rc = 0;

    XLogRecPtr decodeLsn;
    XLogSegNo startSeg;
    XLogSegNo endSeg;
    XLByteToSeg(endLsn, endSeg);

    /* Decode one xlog file at a time to avoid taking up too much memory. */
    while (XLByteLT(startLsn, endLsn)) {
        XLByteToSeg(startLsn, startSeg);

        if (startSeg == endSeg) {
            decodeLsn = endLsn;
        } else if (startSeg < endSeg) {
            XLogSegNoOffsetToRecPtr(startSeg + 1, 0, decodeLsn);
        }

        rc = snprintf_s(sql_cmd, MAXCMDLEN, MAXCMDLEN - 1,
            "select * from pg_logical_get_area_changes('%X/%X','%X/%X',NULL,'%s',NULL);",
            (uint32)(startLsn >> BITS_PER_INT), (uint32)startLsn,
            (uint32)(decodeLsn >> BITS_PER_INT), (uint32)decodeLsn,
            decodePlugin);
        securec_check_ss_c(rc, "", "");

        PGresult *result = PQexec(conn, sql_cmd);
        if (result == NULL || PQresultStatus(result) != PGRES_TUPLES_OK) {
            write_stderr(_("%s: pg_logical_slot_get_changes failed: %s\n"), progname, PQerrorMessage(conn));
            PQclear(result);
            PQfinish(conn);
            exit(1);
        }

        if (!parse_print_decode_result(result)) {
            PQclear(result);
            PQfinish(conn);
            exit(1);
        }
        PQclear(result);

        startLsn = decodeLsn;
    }
}

/*
 * Check CRC of control file
 */
static void check_control_file(ControlFileData* ControlFile)
{
    pg_crc32c crc;

    /* Calculate CRC */
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char*)ControlFile, offsetof(ControlFileData, crc));
    FIN_CRC32C(crc);

    /* And simply compare it */
    if (!EQ_CRC32C(crc, ControlFile->crc)) {
        write_stderr(_("%s: unexpected control file CRC\n"), progname);
    }
}

/*
 * Verify control file contents in the buffer src, and copy it to *ControlFile.
 */
static void digest_control_file(ControlFileData* ControlFile, const char* src)
{
    errno_t errorno = EOK;

    errorno = memcpy_s(ControlFile, sizeof(ControlFileData), src, sizeof(ControlFileData));
    securec_check_c(errorno, "\0", "\0");
    /* Additional checks on control file */
    check_control_file(ControlFile);
}

char* slurpFile(const char* dir, const char* path, size_t* filesize)
{
    int fd = -1;
    char* buffer = NULL;
    struct stat statbuf;
    char fullpath[MAXPGPATH];
    int len;
    int ss_c = 0;
    const int max_file_size = 0xFFFFF;  // max file size = 1M

    ss_c = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", dir, path);
    securec_check_ss_c(ss_c, "\0", "\0");

    if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1) {
        write_stderr(_("%s: could not open file \"%s\" for reading: %s\n"), progname, fullpath, strerror(errno));
        return NULL;
    }

    if (fstat(fd, &statbuf) < 0) {
        (void)close(fd);
        write_stderr(_("%s: could not open file \"%s\" for reading: %s\n"), progname, fullpath, strerror(errno));
        return NULL;
    }

    len = statbuf.st_size;

    if (len < 0 || len > max_file_size) {
        (void)close(fd);
        write_stderr(_("%s: could not read file \"%s\": unexpected file size\n"), progname, fullpath);
        return NULL;
    }

    buffer = (char*)palloc(len + 1);
    if (buffer == NULL) {
        write_stderr(_("%s: buffer allocate failed: out of memory\n"), progname);
        return NULL;
    }

    if (read(fd, buffer, len) != len) {
        (void)close(fd);
        free(buffer);
        buffer = NULL;
        write_stderr(_("%s: could not read file \"%s\": %s\n"), progname, fullpath, strerror(errno));
        return NULL;
    }
    (void)close(fd);

    /* Zero-terminate the buffer. */
    buffer[len] = '\0';

    if (filesize != NULL)
        *filesize = len;
    return buffer;
}

XLogRecPtr DoradoGetSeachPtr()
{
    ControlFileData standbyCtl = {0};
    ControlFileData tempCtl = {0};
    ControlFileData controlFileTarget;
    errno_t errorno = EOK;
    size_t size = 0;
    char *buffer = slurpFile(ss_instance_config.dss.vgname, "pg_control", &size);
    if (buffer == NULL) {
        return InvalidXLogRecPtr;
    }

    for (int i = 0; i < ss_instance_config.dss.interNodeNum; i++) {
        digest_control_file(&tempCtl, (const char *)(buffer + BLCKSZ * i));
        if (tempCtl.checkPoint > standbyCtl.checkPoint) {
            errorno = memcpy_s(&standbyCtl, sizeof(ControlFileData), &tempCtl, sizeof(ControlFileData));
            securec_check_c(errorno, "\0", "\0");
        }
    }
    free(buffer);
    buffer = NULL;

    errorno = memcpy_s(&controlFileTarget, sizeof(ControlFileData), &standbyCtl, sizeof(ControlFileData));
    securec_check_c(errorno, "\0", "\0");

    return controlFileTarget.checkPoint;
}

XLogRecPtr DoradoFindMaxLsn(char *workingPath, char *returnMsg, int msgLen, pg_crc32 *maxLsnCrc)
{
    XLogRecPtr maxLsn = InvalidXLogRecPtr;
    errno_t errorno = EOK;
    XLogRecPtr searchptr = DoradoGetSeachPtr();
    if (XLogRecPtrIsInvalid(searchptr)) {
        return searchptr;
    }

    XLogReaderState *xlogReader = NULL;
    XLogPageReadPrivate readPrivate = {
        .datadir = NULL,
        .tli = 0
    };
    errorno = memset_s(&readPrivate, sizeof(XLogPageReadPrivate), 0, sizeof(XLogPageReadPrivate));
    securec_check_c(errorno, "\0", "\0");
    readPrivate.tli = 1;
    readPrivate.datadir = workingPath;
    xlogReader = XLogReaderAllocate(&SimpleXLogPageRead, &readPrivate);
    if (xlogReader == NULL) {
        errorno = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1,
            "reader allocate failed.\n");
        securec_check_ss_c(errorno, "\0", "\0");
        return InvalidXLogRecPtr;
    }

    XLogRecord *record = NULL;
    char *errorMsg = NULL;
    while (true) {
        record = XLogReadRecordFromAllDir(xlogDirs, xlogDirNum, xlogReader, searchptr, &errorMsg);
        if (record == NULL) {
            break;
        }
        searchptr = InvalidXLogRecPtr;
        *maxLsnCrc = record->xl_crc;
    }

    maxLsn = xlogReader->ReadRecPtr;
    if (XLogRecPtrIsInvalid(maxLsn)) {
        errorno = snprintf_s(returnMsg, XLOG_READER_MAX_MSGLENTH, XLOG_READER_MAX_MSGLENTH - 1, "%s", errorMsg);
        securec_check_ss_c(errorno, "\0", "\0");
    }

    /* Free all opened resources */
    if (xlogReader != NULL) {
        XLogReaderFree(xlogReader);
        xlogReader = NULL;
    }

    return maxLsn;
}

static bool checkIsDigit(const char* arg)
{
    int i = 0;
    while (arg[i] != '\0') {
        if (!isdigit(arg[i])) {
            return false;
        }
        i++;
    }
    return true;
}

static int getOptions(const int argc, char* const* argv)
{
#define NEWHOST 1
#define NEWPORT 2
#define OLDHOST 3
#define OLDPORT 4
#define FORCE 5
    static struct option long_options[] = {{"help", no_argument, NULL, '?'},
        {"dbname", required_argument, NULL, 'd'},
        {"newhost", required_argument, NULL, NEWHOST},
        {"newport", required_argument, NULL, NEWPORT},
        {"oldhost", required_argument, NULL, OLDHOST},
        {"oldport", required_argument, NULL, OLDPORT},
        {"username", required_argument, NULL, 'U'},
        {"password", no_argument, NULL, 'W'},
        {"plugin", required_argument, NULL, 'P'},
        {"file", required_argument, NULL, 'f'},
        {"force", no_argument, NULL, FORCE},
        {NULL, 0, NULL, 0}};

    int c;
    int option_index;

    while ((c = getopt_long(argc, argv, "d:U:WP:f:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'd':
                check_env_value_c(optarg);
                if (dbname) {
                    pfree_ext(dbname);
                }
                dbname = pg_strdup(optarg);
                break;
            case NEWHOST:
                check_env_value_c(optarg);
                if (newHost) {
                    pfree_ext(newHost);
                }
                newHost = pg_strdup(optarg);
                break;
            case NEWPORT:
                check_env_value_c(optarg);
                if (checkIsDigit(optarg) == 0) {
                    write_stderr(_("%s: invalid port number \"%s\"\n"), progname, optarg);
                    return -1;
                }
                newPort = atoi(optarg);
                break;
            case OLDHOST:
                check_env_value_c(optarg);
                if (oldHost) {
                    pfree_ext(oldHost);
                }
                oldHost = pg_strdup(optarg);
                break;
            case OLDPORT:
                check_env_value_c(optarg);
                if (checkIsDigit(optarg) == 0) {
                    write_stderr(_("%s: invalid port number \"%s\"\n"), progname, optarg);
                    return -1;
                }
                oldPort = atoi(optarg);
                break;
            case 'U':
                check_env_value_c(optarg);
                if (userName) {
                    pfree_ext(userName);
                }
                userName = pg_strdup(optarg);
                break;
            case 'W':
                dbgetpassword = 1;
                break;
            case 'P':
                check_env_value_c(optarg);
                if (decodePlugin) {
                    pfree_ext(decodePlugin);
                }
                decodePlugin = pg_strdup(optarg);
                break;
            case 'f':
                check_env_value_c(optarg);
                if (outputFile) {
                    pfree_ext(outputFile);
                }
                outputFile = pg_strdup(optarg);
                break;
            case FORCE:
                doAreaChange = true;
                break;
            default:
                do_advice();
                exit(1);
        }
    }

    /*
     * Any non-option arguments?
     */
    if (argc > optind) {
        write_stderr(_("%s: too many command-line arguments (first is \"%s\")\n"), progname, argv[optind]);
        write_stderr(_("Try \"%s --help\" for more information.\n"), progname);
        return -1;
    }

    return 0;
}

int main(int argc, char **argv)
{
    progname = get_progname(argv[0]);

    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            do_help();
            exit(0);
        }
    }

    if (getOptions(argc, argv) == -1) {
        exit(1);
    }

    /*
     * Required arguments
     */
    if (dbname == NULL) {
        write_stderr(_("%s: no database specified\n"), progname);
        do_advice();
        exit(1);
    }

    if (newHost == NULL) {
        write_stderr(_("%s: no newhost specified\n"), progname);
        do_advice();
        exit(1);
    }

    if (newPort <= 0) {
        write_stderr(_("%s: no newport specified or it must be a true value\n"), progname);
        do_advice();
        exit(1);
    }

    if (oldHost == NULL) {
        write_stderr(_("%s: no oldhost specified\n"), progname);
        do_advice();
        exit(1);
    }

    if (oldPort <= 0) {
        write_stderr(_("%s: no oldport specified or it must be a true value\n"), progname);
        do_advice();
        exit(1);
    }

    if (userName == NULL) {
        write_stderr(_("%s: no username specified\n"), progname);
        do_advice();
        exit(1);
    }

    if (outputFile == NULL) {
        write_stderr(_("%s: no file specified\n"), progname);
        do_advice();
        exit(1);
    }

    if (decodePlugin == NULL) {
        decodePlugin = "mppdb_decoding";
    }

    datadir = getenv("PGDATA");
    if (datadir == NULL) {
        write_stderr(_("%s: no data directory specified\n"
                       "You must identify the directory where the data for this database system "
                       "will reside.  Do this with the environment variable PGDATA.\n"),
            progname);
        exit(1);
    }

    /* init dss if ss_enable_dss is on */
    (void)ss_read_config(datadir);
    if (ss_instance_config.dss.enable_dss) {
        // dss device init
        if (dss_device_init(ss_instance_config.dss.socketpath,
            ss_instance_config.dss.enable_dss) != DSS_SUCCESS) {
            write_stderr(_("%s: failed to init dss device\n"), progname);
            exit(1);
        }

        /* Prepare some g_datadir parameters */
        g_datadir.instance_id = ss_instance_config.dss.instance_id;

        errno_t rc = strcpy_s(g_datadir.dss_data, strlen(ss_instance_config.dss.vgname) + 1,
            ss_instance_config.dss.vgname);
        securec_check_c(rc, "\0", "\0");

        if (ss_instance_config.dss.vglog == NULL) {
            ss_instance_config.dss.vglog = ss_instance_config.dss.vgname;
        }
        rc = strcpy_s(g_datadir.dss_log, strlen(ss_instance_config.dss.vglog) + 1, ss_instance_config.dss.vglog);
        securec_check_c(rc, "\0", "\0");
        
        /* The default of XLogSegmentSize was set 16M during configure, we reassign 1G to XLogSegmentSize
           when dss enable */
        XLogSegmentSize = DSS_XLOG_SEG_SIZE;
    }

    char returnmsg[XLOG_READER_MAX_MSGLENTH] = {0};
    pg_crc32 maxLsnCrc = 0;
    XLogRecPtr max_lsn = InvalidXLogRecPtr;

    if (ss_instance_config.dss.enable_dss) {
        xlogDirNum = SSInitXlogDir(&xlogDirs);
        if (xlogDirNum <= 0) {
            write_stderr(_("%s: init xlog dirs failed\n"), progname);
            exit(1);
        }
        if (ss_instance_config.dss.enable_dorado) {
            max_lsn = DoradoFindMaxLsn(datadir, returnmsg, XLOG_READER_MAX_MSGLENTH, &maxLsnCrc);
        } else {
            max_lsn =
                SSFindMaxLSN(datadir, returnmsg, XLOG_READER_MAX_MSGLENTH, &maxLsnCrc, xlogDirs, xlogDirNum);
        }
    } else {
        max_lsn = FindMaxLSN(datadir, returnmsg, XLOG_READER_MAX_MSGLENTH, &maxLsnCrc);
    }
    if (XLogRecPtrIsInvalid(max_lsn)) {
        write_stderr(_("%s: find max lsn fail, errmsg:%s\n"), progname, returnmsg);
        exit(1);
    }

    XLogReaderState* xlogreader = NULL;
    XLogPageReadPrivate readprivate;
    readprivate.datadir = datadir;
    readprivate.tli = 1;
    xlogreader = XLogReaderAllocate(&SimpleXLogPageRead, &readprivate);
    if (xlogreader == NULL) {
        write_stderr(_("%s: XLogReader allocate failed: out of memory\n"), progname);
        exit(1);
    }

    XLogRecPtr searchptr = max_lsn;
    XLogRecPtr needDecodedMinLsn = InvalidXLogRecPtr;

    /* 1. get diverge lsn and to-be-decoded xids by searching xlog from maxlsn */
    write_log(_("%s: begin to search for diverge lsn.\n"), progname);
    XLogRecPtr divergeLsn = get_diverge_lsn_and_need_decoded_xids(xlogreader, &searchptr, &needDecodedMinLsn);
    if (XLogRecPtrIsInvalid(divergeLsn)) {
        write_stderr(_("%s: can not get valid diverge lsn.\n"), progname);
        XLogReaderFree(xlogreader);
        CloseXlogFile();
        exit(1);
    }
    if (needDecodedXids.size() == 0) {
        write_log(_("%s: nothing to decode.\n"), progname);
        XLogReaderFree(xlogreader);
        CloseXlogFile();
        exit(1);
    }

    /* 2. go on searching to get the actual min begin-lsn of to-be-decoded xids */
    get_need_decoded_min_lsn(xlogreader, &searchptr, &needDecodedMinLsn);

    /* 3. find restartLsn for logical replication slot */
    XLogRecPtr restartLsn = InvalidXLogRecPtr;
    if (!doAreaChange) {
        searchptr = needDecodedMinLsn;
        restartLsn = get_restart_lsn(xlogreader, &searchptr);
        if (XLogRecPtrIsInvalid(restartLsn)) {
            write_log(_("%s: cannot find proper restart_lsn, so change to do area change.\n"), progname);
            doAreaChange = true;
        } else {
            write_log(_("%s: found proper restart_lsn %X/%X.\n"), progname,
                (uint32)(restartLsn >> BITS_PER_INT), (uint32)restartLsn);
        }
    }

    XLogReaderFree(xlogreader);
    CloseXlogFile();

    /* 4. do logical replication */
    write_log(_("%s: ready to connect old-master database and do logical replication.\n"), progname);
    PGconn* localConn = get_local_connection();
    if (localConn == NULL) {
        exit(1);
    }

    if (doAreaChange) {
        logical_replication_for_area_decode(localConn, needDecodedMinLsn, max_lsn);
    } else {
        logical_replication_for_retrieve(localConn, restartLsn, divergeLsn);
    }
    PQfinish(localConn);

    write_log(_("%s: retrieve data finished.\n"), progname);
}
