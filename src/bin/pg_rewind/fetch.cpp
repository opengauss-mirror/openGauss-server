/* -------------------------------------------------------------------------
 *
 * libpq_fetch.c
 *	  Functions for fetching files from a remote server.
 *
 * Copyright (c) 2013-2015, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>

/* for ntohl/htonl */
#include <arpa/inet.h>

#include "pg_rewind.h"
#include "fetch.h"
#include "file_ops.h"
#include "logging.h"
#include "pg_build.h"

#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "common/fe_memutils.h"
#include "catalog/catalog.h"
#include "PageCompression.h"
#include "catalog/pg_type.h"
#include "storage/file/fio_device.h"
#include "access/extreme_rto/standby_read/standby_read_base.h"

PGconn* conn = NULL;
char source_slot_name[NAMEDATALEN] = {0};
char log_directory[MAXPGPATH] = {0};

/*
 * Files are fetched max CHUNKSIZE bytes at a time.
 *
 * (This only applies to files that are copied in whole, or for truncated
 * files where we copy the tail. Relation files, where we know the individual
 * blocks that need to be fetched, are fetched in BLCKSZ chunks.)
 */
#define CHUNKSIZE (1024 * 1024)
#define BLOCKSIZE (8 * 1024)
const uint64 MAX_FILE_SIZE = 0xFFFFFFFF;
#ifndef WIN32
#define _atoi64(val) strtol(val, NULL, 10)
#endif

#define INVALID_LINES_IDX (int)(~0)
#define MAX_PARAM_LEN 1024
#define FH_CUSTOM_VALUE_FOUR 4
#define FH_CUSTOM_VALUE_FIVE 5

static BuildErrorCode receiveFileChunks(const char* sql, FILE* file);
static BuildErrorCode execute_pagemap(file_entry_t* entry, FILE* file);
static char* run_simple_query(const char* sql);
static BuildErrorCode recurse_dir(const char* datadir, const char* path, process_file_callback_t callback);
static void get_slot_name_by_app_name(void);
static BuildErrorCode CheckResultSet(PGresult* pgResult);
static void get_log_directory_guc(void);
static void *ProgressReportIncrementalBuild(void *arg);

static volatile bool g_progressFlag = false;
static pthread_cond_t g_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;

BuildErrorCode libpqConnect(const char* connstr)
{
    PGresult* res = NULL;

    conn = PQconnectdb(connstr);
    if (PQstatus(conn) == CONNECTION_BAD) {
        pg_log(PG_ERROR, "could not connect to server %s: %s", connstr, PQerrorMessage(conn));
        return BUILD_ERROR;
    }

    pg_log(PG_PROGRESS, "connected to server: %s\n", connstr);

    res = PQexec(conn, "SET xc_maintenance_mode to on");
    PQclear(res);

    res = PQexec(conn, "SET session_timeout = 0");
    PQclear(res);

    res = PQexec(conn, "SET statement_timeout = 0");
    PQclear(res);
    return BUILD_SUCCESS;
}

void libpqDisconnect()
{
    PQfinish(conn);
    conn = NULL;
}

BuildErrorCode libpqGetParameters(void)
{
    char* str = NULL;
    char* str2 = NULL;
    errno_t ss_c = EOK;

    /*
     * Check that the server is not in hot standby mode. There is no
     * fundamental reason that couldn't be made to work, but it doesn't
     * currently because we use a temporary table. Better to check for it
     * explicitly than error out, for a better error message.
     */
    str = run_simple_query("SELECT pg_is_in_recovery()");
    if (str == NULL) {
        return BUILD_FATAL;
    }
    if (strcmp(str, "f") != 0) {
        pg_log(PG_ERROR, "source server must not be in recovery mode\n");
        pg_free(str);
        str = NULL;
        return BUILD_ERROR;
    }
    pg_free(str);
    str = NULL;

    /*
     * Also check that full_page_writes is enabled. We can get torn pages if
     * a page is modified while we read it with pg_read_binary_file(), and we
     * rely on full page images to fix them.
     */
    str = run_simple_query("SHOW full_page_writes");
    if (str == NULL) {
        return BUILD_FATAL;
    }
    str2 = run_simple_query("SHOW enable_incremental_checkpoint");
    if (str2 == NULL) {
        pg_free(str);
        str = NULL;
        return BUILD_FATAL;
    }
    if (strcmp(str, "on") != 0 && strcmp(str2, "on") != 0) {
        pg_fatal(
            "full_page_writes must be enabled in the source server when the incremental checkpoint is not used.\n");
        pg_free(str);
        pg_free(str2);
        str = NULL;
        str2 = NULL;
        return BUILD_FATAL;
    }
    pg_free(str);
    pg_free(str2);
    str = NULL;
    str2 = NULL;

    /* Get the pgxc_node_name */
    str = run_simple_query("SHOW pgxc_node_name");
    if (str == NULL) {
        return BUILD_FATAL;
    }
    ss_c = snprintf_s(pgxcnodename, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", str);
    securec_check_ss_c(ss_c, "\0", "\0");
    pg_free(str);
    str = NULL;

    pg_log(PG_DEBUG, "pgxc_node_name is %s.\n", pgxcnodename);

    /*
     * Also check that replication_type. We generate the replication slot names
     * 	under this parameter.
     */
    str = run_simple_query("SHOW replication_type");
    if (str == NULL) {
        return BUILD_FATAL;
    }
    replication_type = atoi(str);
    pg_free(str);
    str = NULL;

    pg_log(PG_DEBUG, "replication_type is %d.\n", replication_type);
    return BUILD_SUCCESS;
}

/*
 * Runs a query that returns a single value.
 */
static char* run_simple_query(const char* sql)
{
    PGresult* res = NULL;
    char* result = NULL;

    res = PQexec(conn, sql);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_fatal("error running query (%s) in source server: %s", sql, PQresultErrorMessage(res));
        goto error;
    }
    /* sanity check the result set */
    if (PQnfields(res) != 1 || PQntuples(res) != 1 || PQgetisnull(res, 0, 0)) {
        pg_fatal("unexpected result set from query\n");
        goto error;
    }

    result = pg_strdup(PQgetvalue(res, 0, 0));

    PQclear(res);

    return result;

error:
    PQclear(res);
    return result;
}

/*
 * Request checkpoint function
 */
void libpqRequestCheckpoint(void)
{
    PGresult* res = NULL;

    res = PQexec(conn, "CHECKPOINT;");
    PQclear(res);
}

/*
 * Calls pg_current_xlog_insert_location() function
 */
XLogRecPtr libpqGetCurrentXlogInsertLocation(void)
{
    XLogRecPtr result = InvalidXLogRecPtr;
    uint32 hi = 0;
    uint32 lo = 0;
    char* val = NULL;

    val = run_simple_query("SELECT gs_current_xlog_insert_end_location()");
    if (val == NULL) {
        pg_fatal("Could not get source flush lsn.\n");
        goto error;
    }
    if (sscanf_s(val, "%X/%X", &hi, &lo) != 2) {
        pg_fatal("unrecognized result \"%s\" for current XLOG insert location\n", val);
        goto error;
    }
    result = ((uint64)hi) << 32 | lo;

    free(val);
    val = NULL;

    return result;

error:
    free(val);
    return result;
}

/*
 * Get a list of all files in the data directory.
 */
BuildErrorCode fetchSourceFileList()
{
    PGresult* res = NULL;
    const char* sql = NULL;
    int i;
    BuildErrorCode rv = BUILD_SUCCESS;

    /*
     * Create a recursive directory listing of the whole data directory.
     *
     * The WITH RECURSIVE part does most of the work. The second part gets the
     * targets of the symlinks in pg_tblspc directory.
     *
     * XXX: There is no backend function to get a symbolic link's target in
     * general, so if the admin has put any custom symbolic links in the data
     * directory, they won't be copied correctly.
     */
    /* skip pca/pcd files and concat pca with table file */
    sql = "WITH tmp_table AS (\n"
          "SELECT path, size, isdir, pg_tablespace_location(pg_tablespace.oid) AS link_target \n"
          "FROM (SELECT * FROM pg_stat_file_recursive('.')) AS files \n"
          "LEFT OUTER JOIN pg_tablespace ON files.path ~ '^pg_tblspc/' AND oid :: text = files.filename\n"
          ")"
          "SELECT path, size, isdir, link_target,\n"
          "CASE WHEN path ~ '" COMPRESS_STR "$' THEN pg_read_binary_file(path, size - %u, %d, true)\n"
          "END AS pchdr\n"
          "FROM tmp_table\n";
    char sqlbuf[1024];
    int rc = snprintf_s(sqlbuf, sizeof(sqlbuf), sizeof(sqlbuf) - 1, sql, BLCKSZ, sizeof(CfsExtentHeader));
    securec_check_ss_c(rc, "\0", "\0");
    res = PQexec(conn, (const char*)sqlbuf);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_ERROR, "could not fetch file list: %s", PQresultErrorMessage(res));
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }

    /* sanity check the result set */
    if (PQnfields(res) != 5) {
        PQclear(res);
        res = NULL;
        pg_fatal("unexpected result set while fetching file list\n");
    }

    /* wait for local stat */
    rv = waitEndTargetFileStatThread();
    PG_CHECKRETURN_AND_FREE_PGRESULT_RETURN(rv, res);
    /* Read result to local variables */
    for (i = 0; i < PQntuples(res); i++) {
        char* path = PQgetvalue(res, i, 0);
        int64 filesize = _atoi64(PQgetvalue(res, i, 1));
        bool isdir = (strcmp(PQgetvalue(res, i, 2), "t") == 0);
        char* link_target = PQgetvalue(res, i, 3);
        file_type_t type;
        if (NULL != strstr(path, "pg_rewind_bak"))
            continue;
        if (NULL != strstr(path, "disable_conn_file"))
            continue;
        if (NULL != strstr(path, EXRTO_FILE_DIR)) {
            continue;
        }

        if (PQgetisnull(res, 0, 1)) {
            /*
             * The file was removed from the server while the query was
             * running. Ignore it.
             */
            continue;
        }

        if (link_target[0])
            type = FILE_TYPE_SYMLINK;
        else if (isdir)
            type = FILE_TYPE_DIRECTORY;
        else {
            type = FILE_TYPE_REGULAR;
            if ((filesize % BLOCKSIZE) != 0) {
                bool isrowfile;
                isrowfile = isRelDataFile(path);
                if (isrowfile) {
                    filesize = filesize - (filesize % BLOCKSIZE);
                    pg_log(PG_PROGRESS, "source filesize mod BLOCKSIZE not equal 0 %s %s \n", path, PQgetvalue(res, i, 1));
                }
            }
        }
        RewindCompressInfo rewindCompressInfo;
        RewindCompressInfo *pointer = NULL;
        if (!PQgetisnull(res, i, FH_CUSTOM_VALUE_FOUR)) {
            size_t length = 0;
            unsigned char *ptr = PQunescapeBytea((const unsigned char*)PQgetvalue(res, i, 4), &length);
            if (FetchSourcePca(ptr, length, &rewindCompressInfo, (size_t)filesize)) {
                filesize = rewindCompressInfo.newBlockNumber * BLCKSZ;
                pointer = &rewindCompressInfo;
            }
            PQfreemem(ptr);
        }
        process_source_file(path, type, filesize, link_target, pointer);
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }
    PQclear(res);
    return BUILD_SUCCESS;
}

/* ----
 * Runs a query, which returns pieces of files from the remote source data
 * directory, and overwrites the corresponding parts of target files with
 * the received parts. The result set is expected to be of format:
 *
 * path		text	-- path in the data directory, e.g "base/1/123"
 * begin	int4	-- offset within the file
 * chunk	bytea	-- file content
 * ----
 */
static BuildErrorCode receiveFileChunks(const char* sql, FILE* file)
{
    PGresult* res = NULL;
    errno_t errorno = EOK;

    if (PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 1) != 1) {
        pg_fatal("could not send query: %s", PQerrorMessage(conn));
        return BUILD_FATAL;
    }

    pg_log(PG_DEBUG, "getting file chunks\n");

    if (PQsetSingleRowMode(conn) != 1) {
        pg_fatal("could not set libpq connection to single row mode\n");
        return BUILD_FATAL;
    }

    fprintf(stdout, "Begin fetching files \n");
    pthread_t progressThread;
    pthread_create(&progressThread, NULL, ProgressReportIncrementalBuild, NULL);

    while ((res = PQgetResult(conn)) != NULL) {
        char* filename = NULL;
        int filenamelen;
        int chunkoff;
        int chunksize;
        int chunkspace;
        char* chunk = NULL;

        switch (PQresultStatus(res)) {
            case PGRES_SINGLE_TUPLE:
                break;

            case PGRES_TUPLES_OK:
                PQclear(res);
                continue; /* final zero-row result */

            default:
                pg_fatal("unexpected result while fetching remote files: %s\n", PQresultErrorMessage(res));
                PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
        }

        /* sanity check the result set */
        if (PQnfields(res) != 7 || PQntuples(res) != 1) {
            pg_fatal("unexpected result set size while fetching remote files\n");
            PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
        }

        if (PQftype(res, 0) != TEXTOID && PQftype(res, 1) != INT4OID && PQftype(res, 2) != BYTEAOID &&
            PQftype(res, 3) != INT4OID) {
            pg_fatal("unexpected data types in result set while fetching remote files: %u %u %u %u\n",
                PQftype(res, 0),
                PQftype(res, 1),
                PQftype(res, 2),
                PQftype(res, 3));
            PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
        }

        if (PQfformat(res, 0) != 1 && PQfformat(res, 1) != 1 && PQfformat(res, 2) != 1 && PQfformat(res, 3) != 1) {
            pg_fatal("unexpected result format while fetching remote files\n");
            PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
        }

        if (PQgetisnull(res, 0, 0) || PQgetisnull(res, 0, 1) || PQgetisnull(res, 0, 3)) {
            pg_fatal("unexpected null values in result while fetching remote files\n");
            PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
        }

        if (PQgetlength(res, 0, 1) != sizeof(int32)) {
            pg_fatal("unexpected result length while fetching remote files\n");
            PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
        }
        /* check compressed result set */
        (void)CheckResultSet(res);

        /* Read result set to local variables */
        errorno = memcpy_s(&chunkoff, sizeof(int32), PQgetvalue(res, 0, 1), sizeof(int32));
        securec_check_c(errorno, "\0", "\0");
        chunkoff = ntohl(chunkoff);
        chunksize = PQgetlength(res, 0, 2);

        filenamelen = PQgetlength(res, 0, 0);
        filename = (char*)pg_malloc(filenamelen + 1);
        errorno = memcpy_s(filename, filenamelen + 1, PQgetvalue(res, 0, 0), filenamelen);
        securec_check_c(errorno, "\0", "\0");
        filename[filenamelen] = '\0';

        chunk = PQgetvalue(res, 0, 2);
        errorno = memcpy_s(&chunkspace, sizeof(int32), PQgetvalue(res, 0, 3), sizeof(int32));
        securec_check_c(errorno, "\0", "\0");
        chunkspace = ntohl(chunkspace);

        /*
         * If a file has been deleted on the source, remove it on the target
         * as well.  Note that multiple unlink() calls may happen on the same
         * file if multiple data chunks are associated with it, hence ignore
        * unconditionally anything missing.  If this file is not a relation
         * data file, then it has been already truncated when creating the
         * file chunk list at the previous execution of the filemap.
         */
        if (PQgetisnull(res, 0, 2)) {
            pg_log(PG_DEBUG, "received null value for chunk for file \"%s\", file has been deleted", filename);
            remove_target_file(filename, true);
            pg_free(filename);
            filename = NULL;
            PQclear(res);
            res = NULL;
            continue;
        }

        int32 algorithm;
        errorno = memcpy_s(&algorithm, sizeof(int32), PQgetvalue(res, 0, 4), sizeof(int32));
        securec_check_c(errorno, "\0", "\0");
        pg_log(PG_DEBUG, "received chunk for file \"%s\", offset %d, size %d\n", filename, chunkoff, chunksize);
        fprintf(file, "received chunk for file \"%s\", offset %d, size %d\n", filename, chunkoff, chunksize);
        algorithm = (int32)ntohl((uint32)algorithm);
        if (algorithm == 0) {
            open_target_file(filename, false);
            pg_free(filename);
            filename = NULL;
            PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
            write_target_range(chunk, chunkoff, chunksize, chunkspace);
        } else {
            int32 chunkSize;
            errorno = memcpy_s(&chunkSize, sizeof(int32), PQgetvalue(res, 0, FH_CUSTOM_VALUE_FIVE), sizeof(int32));
            securec_check_c(errorno, "\0", "\0");
            chunkSize = (int32)ntohl((uint32)chunkSize);
            bool rebuild = *PQgetvalue(res, 0, 6) != 0;
            CompressedFileInit(filename, rebuild);
            pg_free(filename);
            filename = NULL;
            /* fetch result */
            FetchCompressedFile(chunk, (uint32)chunkoff, (int32)chunkspace, (uint16)chunkSize, (uint8)algorithm);
        }
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }

    g_progressFlag = true;
    pthread_mutex_lock(&g_mutex);
    pthread_cond_signal(&g_cond);
    pthread_mutex_unlock(&g_mutex);
    pthread_join(progressThread, NULL);

    fprintf(stdout, "Finish fetching files \n");

    return BUILD_SUCCESS;
}

/**
 * check result set of compressed tables
 * @param pgResult result
 * @return success or not
 */
static BuildErrorCode CheckResultSet(PGresult* res)
{
#define PQ_TYPE(index, type) (PQftype(res, (index)) != (type))
    if (PQ_TYPE(4, INT4OID) || PQ_TYPE(5, INT4OID) || PQ_TYPE(6, BOOLOID)) {
        pg_fatal(
            "FetchCompressedFile:unexpected data types: %u %u %u\n", PQftype(res, 4), PQftype(res, 5), PQftype(res, 6));
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }
#define PQ_FORMAT(index) (PQfformat(res, 0) != 1)
    if (PQ_FORMAT(4) && PQ_FORMAT(5) && PQ_FORMAT(6)) {
        pg_fatal("unexpected result format while fetching remote files\n");
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }
#define PQ_ISNULL(index) (PQgetisnull(res, 0, (index)))
    if (PQ_ISNULL(4) || PQ_ISNULL(5) || PQ_ISNULL(6)) {
        pg_fatal("unexpected null values in result while fetching remote files\n");
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }
    return BUILD_SUCCESS;
}

/*
 * Receive a single file as a malloc'd buffer.
 */
char* fetchFile(char* filename, size_t* filesize)
{
    PGresult* res = NULL;
    char* result = NULL;
    int len;
    const char* paramValues[1];
    errno_t errorno = EOK;
    const int max_file_size = 0xFFFFF;  // max file size = 1M

    paramValues[0] = filename;
    res = PQexecParams(conn, "SELECT pg_read_binary_file($1)", 1, NULL, paramValues, NULL, NULL, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_ERROR, "could not fetch remote file \"%s\": %s", filename, PQresultErrorMessage(res));
        goto error;
    }

    /* sanity check the result set */
    if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0)) {
        pg_fatal("unexpected result set while fetching remote file \"%s\"\n", filename);
        goto error;
    }

    /* Read result to local variables */
    len = PQgetlength(res, 0, 0);
    if (len < 0 || len > max_file_size) {
        pg_fatal("unexpected file size of remote file \"%s\"\n", filename);
        goto error;
    }
    result = (char*)pg_malloc(len + 1);
    errorno = memcpy_s(result, len + 1, PQgetvalue(res, 0, 0), len);
    securec_check_c(errorno, "\0", "\0");
    result[len] = '\0';

    pg_log(PG_DEBUG, "fetched file \"%s\", length %d\n", filename, len);

    if (filesize != NULL)
        *filesize = len;

    PQclear(res);
    res = NULL;

    return result;

error:
    PQclear(res);
    res = NULL;
    return result;
}

static void CompressedFileCopy(const file_entry_t* entry, bool rebuild)
{
    Assert(!rebuild || entry->rewindCompressInfo.oldBlockNumber == 0);
    if (dry_run) {
        return;
    }

    char linebuf[MAXPGPATH + 47];
    int ret = snprintf_s(linebuf,
        sizeof(linebuf),
        sizeof(linebuf) - 1,
        "%s\t%u\t%u\t%u\n",
        entry->path,
        entry->rewindCompressInfo.oldBlockNumber,
        entry->rewindCompressInfo.newBlockNumber - entry->rewindCompressInfo.oldBlockNumber,
        rebuild);
    securec_check_ss_c(ret, "\0", "\0");
    if (PQputCopyData(conn, linebuf, (int)strlen(linebuf)) != 1) {
        pg_fatal("could not send COPY data: %s", PQerrorMessage(conn));
    }
    pg_log(PG_DEBUG, "CompressedFileCopy: %s", linebuf);
}

static void CompressedFileRemove(const file_entry_t* entry)
{
    char path[MAXPGPATH];
    pg_log(PG_DEBUG, "CompressedFileRemove: %s\n", path);
    error_t rc = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/" COMPRESS_SUFFIX, pg_data, entry->path);
    securec_check_ss_c(rc, "\0", "\0");
    pg_log(PG_DEBUG, "CompressedFileRemove: %s\n", path);
    if (unlink(path) != 0) {
        if (errno == ENOENT) {
            return;
        }
        pg_fatal("could not remove compress file \"%s\": %s\n", path, strerror(errno));
        return;
    }
}

/*
 * Write a file range to a temporary table in the server.
 *
 * The range is sent to the server as a COPY formatted line, to be inserted
 * into the 'fetchchunks' temporary table. It is used in receiveFileChunks()
 * function to actually fetch the data.
 */
static void fetch_file_range(const char* path, unsigned int begin, unsigned int end)
{
    char linebuf[MAXPGPATH + 47];
    int ss_c = 0;

    /* Split the range into CHUNKSIZE chunks */
    while (end - begin > 0) {
        unsigned int len;

        if (end - begin > CHUNKSIZE) {
            len = CHUNKSIZE;
        } else {
            len = end - begin;
        }
        ss_c = snprintf_s(
            linebuf, sizeof(linebuf), sizeof(linebuf) - 1, "%s\t%u\t%u\t%u\n", path, begin, len, 0);
        securec_check_ss_c(ss_c, "\0", "\0");

        if (PQputCopyData(conn, linebuf, strlen(linebuf)) != 1)
            pg_fatal("could not send COPY data: %s", PQerrorMessage(conn));
        begin += len;
    }
}

/*
 * Fetch all changed blocks from remote source data directory.
 */
BuildErrorCode executeFileMap(filemap_t* map, FILE* file)
{
    file_entry_t* entry = NULL;
    const char* sql = NULL;
    PGresult* res = NULL;
    int i;

    /*
     * First create a temporary table, and load it with the blocks that we
     * need to fetch.
     */
    sql = "CREATE TEMPORARY TABLE fetchchunks(path text, begin int4, len int4, rebuild bool);";
    res = PQexec(conn, sql);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        pg_fatal("could not create temporary table: %s", PQresultErrorMessage(res));
        PQclear(res);
        res = NULL;
        return BUILD_FATAL;
    }
    PQclear(res);
    res = NULL;

    sql = "set enable_data_replicate= off; COPY fetchchunks FROM STDIN";
    res = PQexec(conn, sql);

    if (PQresultStatus(res) != PGRES_COPY_IN) {
        pg_fatal("could not send file list: %s", PQresultErrorMessage(res));
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }

    fprintf(file, "copy file list to temporary table fetchchunks.\n");
    for (i = 0; i < map->narray; i++) {
        entry = map->array[i];

        if ((IS_CROSS_CLUSTER_BUILD && strcmp(entry->path, "pg_hba.conf") == 0) ||
            strcmp(entry->path, "pg_hba.conf.old") == 0) {
            continue;
        }

        /* report all the path to check whether it's correct */
        if (entry->rewindCompressInfo.compressed) {
            pg_log(PG_PROGRESS, "path: %s, type: %d, action: %d\n", entry->path, (int)entry->type, (int)entry->action);

        }

        pg_log(PG_DEBUG, "path: %s, type: %d, action: %d\n", entry->path, (int)entry->type, (int)entry->action);
        (void)fprintf(file, "path: %s, type: %d, action: %d\n", entry->path, (int)entry->type, (int)entry->action);

        /* If this is a relation file, copy the modified blocks */
        bool compressed = entry->rewindCompressInfo.compressed;
        (void)execute_pagemap(entry, file);
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);

        switch (entry->action) {
            case FILE_ACTION_NONE:
                /* nothing else to do */
                break;

            case FILE_ACTION_COPY:
                if (compressed) {
                    CompressedFileCopy(entry, true);
                    PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
                } else {
                    /* Truncate the old file out of the way, if any */
                    open_target_file(entry->path, true);
                    PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
                    fetch_file_range(entry->path, 0, entry->newsize);
                    PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
                }
                break;

            case FILE_ACTION_TRUNCATE:
                if (compressed) {
                    CompressedFileTruncate(entry->path, &entry->rewindCompressInfo);
                } else {
                    truncate_target_file(entry->path, entry->newsize);
                }
                PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
                break;

            case FILE_ACTION_COPY_TAIL:
                if (compressed) {
                    CompressedFileCopy(entry, false);
                } else {
                    fetch_file_range(entry->path, entry->oldsize, entry->newsize);
                }
                PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
                break;

            case FILE_ACTION_REMOVE:
                if (compressed) {
                    CompressedFileRemove(entry);
                } else {
                    remove_target(entry);
                }
                PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
                break;

            case FILE_ACTION_CREATE:
                Assert(!compressed);
                create_target(entry);
                PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
                break;

            default:
                break;
        }
    }

    if (PQputCopyEnd(conn, NULL) != 1) {
        pg_fatal("could not send end-of-COPY: %s", PQerrorMessage(conn));
        PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
    }

    PQclear(res);
    res = NULL;

    while ((res = PQgetResult(conn)) != NULL) {
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            pg_fatal("unexpected result while sending file list: %s", PQresultErrorMessage(res));
            PG_CHECKBUILD_AND_FREE_PGRESULT_RETURN(res);
        }
        PQclear(res);
        res = NULL;
    }

    /*
     * We've now copied the list of file ranges that we need to fetch to the
     * temporary table. Now, actually fetch all of those ranges.
     */

    sql = "SELECT path, begin, \n"
          "  pg_read_binary_file(path, begin, len, true) AS chunk, len, 0 as algorithm, 0 as chunksize,rebuild \n"
          "FROM fetchchunks where path !~ '" COMPRESS_STR "$' \n"
          "union all \n"
          "(select (json->>'path')::text as path, (json->>'blocknum')::int4 as begin,"
          " (json->>'data')::bytea as chunk,\n"
          "(json->>'len')::int4 as len, \n"
          "(json->>'algorithm')::int4 as algorithm, (json->>'chunksize')::int4 as chunksize ,rebuild \n"
          "from (select row_to_json(pg_read_binary_file_blocks(path,begin,len)) json,rebuild \n"
          "from fetchchunks  where path ~ '" COMPRESS_STR "$') \n"
          "order by path, begin);";

    fprintf(file, "fetch and write file based on temporary table fetchchunks.\n");
    return receiveFileChunks(sql, file);
}

/*
 * Backup all relevant blocks from local target data directory.
 */
BuildErrorCode backupFileMap(filemap_t* map)
{
    file_entry_t* entry = NULL;
    int i;
    int fd = -1;
    int nRet = 0;
    char bkup_file[MAXPGPATH] = {0};
    char bkup_start_file[MAXPGPATH] = {0};
    char bkup_done_file[MAXPGPATH] = {0};

    /*
     * Create backup tag file to indicate the completeness of backup.
     */
    nRet = snprintf_s(bkup_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, "pg_rewind_bak");
    securec_check_ss_c(nRet, "", "");
    nRet = snprintf_s(bkup_start_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", bkup_file, TAG_BACKUP_START);
    securec_check_ss_c(nRet, "\0", "\0");
    nRet = snprintf_s(bkup_done_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", bkup_file, TAG_BACKUP_DONE);
    securec_check_ss_c(nRet, "\0", "\0");

    if (pg_mkdir_p(bkup_file, S_IRWXU) != 0) {
        pg_fatal("could not create directory \"%s\": %s\n", bkup_file, gs_strerror(errno));
        return BUILD_FATAL;
    }

    canonicalize_path(bkup_start_file);
    if ((fd = open(bkup_start_file, O_WRONLY | O_CREAT | O_EXCL, 0600)) < 0) {
        pg_fatal("could not create file \"%s\\%s\": %s\n", bkup_file, TAG_BACKUP_START, gs_strerror(errno));
        return BUILD_FATAL;
    }
    close(fd);
    fd = -1;

    for (i = 0; i < map->narray; i++) {
        entry = map->array[i];

        /* report all the path to check whether it's correct */
        pg_log(PG_DEBUG, "path: %s, type: %d, action: %d\n", entry->path, entry->type, entry->action);

        switch (entry->action) {
            case FILE_ACTION_NONE:
                /* for entry with page map not null, back up corresponding file */
                if (entry->pagemap.bitmapsize > 0) {
                    backup_target_file(entry->path, divergeXlogFileName);
                }
                break;

            case FILE_ACTION_CREATE:
                /* to be supported later */
                break;

            case FILE_ACTION_COPY: {
                /* create fake file for restore when file not exist, otherwise, backup file */
                file_entry_t statbuf;
                if (targetFilemapSearch(entry->path, &statbuf) < 0) {
                    backup_fake_target_file(entry->path);
                } else {
                    backup_target_file(entry->path, divergeXlogFileName);
                }
                break;
            }

            case FILE_ACTION_COPY_TAIL:
            case FILE_ACTION_TRUNCATE:
                backup_target_file(entry->path, divergeXlogFileName);
                break;

            case FILE_ACTION_REMOVE:
                backup_target(entry, divergeXlogFileName);
                break;

            default:
                break;
        }
    }

    /* rename tag file to done */
    if (rename(bkup_start_file, bkup_done_file) < 0) {
        pg_fatal("failed to rename \"%s\" to \"%s\": %s\n", TAG_BACKUP_START, TAG_BACKUP_DONE, gs_strerror(errno));
        return BUILD_FATAL;
    }
    return BUILD_SUCCESS;
}

/**
 * combine continue blocks numbers and copy file
 * @param entry file entry
 * @param file file
 */
static void CompressedFileCopy(file_entry_t* entry, FILE* file)
{
    datapagemap_t* pagemap = &entry->pagemap;
    datapagemap_iterator_t* iter = datapagemap_iterate(pagemap);

    BlockNumber blkno;
    file_entry_t fileEntry;
    fileEntry.path = entry->path;
    fileEntry.rewindCompressInfo = entry->rewindCompressInfo;
    int invalidNumber = -1;
    long int before = invalidNumber;
    while (datapagemap_next(iter, &blkno)) {
        (void)fprintf(file, "  block %u\n", blkno);
        if (before == -1) {
            fileEntry.rewindCompressInfo.oldBlockNumber = blkno;
            before = blkno;
        } else {
            if (before == blkno - 1) {
                before = blkno;
            } else {
                fileEntry.rewindCompressInfo.newBlockNumber = (uint32)before + 1;
                CompressedFileCopy(&fileEntry, false);
                fileEntry.rewindCompressInfo.oldBlockNumber = blkno;
                before = blkno;
            }
        }
    }
    if (before != invalidNumber) {
        fileEntry.rewindCompressInfo.newBlockNumber = (uint32)before + 1;
        CompressedFileCopy(&fileEntry, false);
    }
    pg_free(iter);
}
static BuildErrorCode execute_pagemap(file_entry_t* entry, FILE* file)
{
    datapagemap_iterator_t* iter = NULL;
    BlockNumber blkno;
    off_t offset;

    datapagemap_t* pagemap = &entry->pagemap;
    char* path = entry->path;
    iter = datapagemap_iterate(pagemap);
    if (entry->rewindCompressInfo.compressed) {
        CompressedFileCopy(entry, file);
    } else {
        while (datapagemap_next(iter, &blkno)) {
            fprintf(file, "  block %u\n", blkno);
            offset = blkno * BLCKSZ;
            fetch_file_range(path, offset, offset + BLCKSZ);
        }
    }
    pg_free(iter);
    iter = NULL;
    return BUILD_SUCCESS;
}

/*
 * Traverse through all files in a data directory, calling 'callback'
 * for each file.
 */
BuildErrorCode traverse_datadir(const char* datadir, process_file_callback_t callback)
{
    get_log_directory_guc();
    recurse_dir(datadir, NULL, callback);
    PG_CHECKBUILD_AND_RETURN();
    return BUILD_SUCCESS;
}

/*
 * recursive part of traverse_datadir
 *
 * parentpath is the current subdirectory's path relative to datadir,
 * or NULL at the top level.
 */
static BuildErrorCode recurse_dir(const char* datadir, const char* parentpath, process_file_callback_t callback)
{
    DIR* xldir = NULL;
    struct dirent* xlde = NULL;
    char fullparentpath[MAXPGPATH];
    int ss_c = 0;

    if (parentpath != NULL)
        ss_c = sprintf_s(fullparentpath, MAXPGPATH, "%s/%s", datadir, parentpath);
    else
        ss_c = sprintf_s(fullparentpath, MAXPGPATH, "%s", datadir);
    securec_check_ss_c(ss_c, "\0", "\0");

    xldir = opendir(fullparentpath);
    if (xldir == NULL) {
        pg_fatal("could not open directory \"%s\": %s\n", fullparentpath, strerror(errno));
        return BUILD_FATAL;
    }

    while (errno = 0, (xlde = readdir(xldir)) != NULL) {
        struct stat fst;
        char fullpath[MAXPGPATH];
        char path[MAXPGPATH];

        if (strcmp(xlde->d_name, ".") == 0 || strcmp(xlde->d_name, "..") == 0)
            continue;
        /* Skip compressed page files */
        size_t dirNamePath = strlen(xlde->d_name);
        if (PageCompression::SkipCompressedFile(xlde->d_name, dirNamePath)) {
            continue;
        }

        if (strcmp(xlde->d_name, log_directory) == 0) {
            pg_log(PG_WARNING, "skip log directory %s during fetch target file list\n", xlde->d_name);
            continue;
        }

        ss_c = snprintf_s(fullpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", fullparentpath, xlde->d_name);
        securec_check_ss_c(ss_c, "\0", "\0");

        if (lstat(fullpath, &fst) < 0) {
            if (errno == ENOENT) {
                /*
                 * File doesn't exist anymore. This is ok, if the new master
                 * is running and the file was just removed. If it was a data
                 * file, there should be a WAL record of the removal. If it
                 * was something else, it couldn't have been anyway.
                 *
                 */
            } else {
                pg_fatal("could not stat file \"%s\" in recurse_dir: %s\n", fullpath, strerror(errno));
                (void)closedir(xldir);
                return BUILD_FATAL;
            }
        }

        if (parentpath != NULL)
            ss_c = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", parentpath, xlde->d_name);
        else
            ss_c = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s", xlde->d_name);
        securec_check_ss_c(ss_c, "\0", "\0");

        if (NULL != strstr(path, "pg_rewind_bak"))
            continue;

        if (S_ISREG(fst.st_mode)) {
            uint64 fileSize = (uint64)fst.st_size;
            RewindCompressInfo rewindCompressInfo;
            RewindCompressInfo *pointer = NULL;
            if (PageCompression::SkipCompressedFile(xlde->d_name, strlen(xlde->d_name)) &&
                ProcessLocalPca(path, &rewindCompressInfo, pg_data)) {
                fileSize = rewindCompressInfo.oldBlockNumber * BLCKSZ;
                pointer = &rewindCompressInfo;
            }
            if (fileSize <= MAX_FILE_SIZE) {
                callback(path, FILE_TYPE_REGULAR, fileSize, NULL, pointer);
                if (increment_return_code != BUILD_SUCCESS) {
                    (void)closedir(xldir);
                }
                PG_CHECKBUILD_AND_RETURN();
            } else {
                pg_log(PG_WARNING, "file size of \"%s\" is over %ld\n", fullpath, MAX_FILE_SIZE);
            }
        } else if (S_ISDIR(fst.st_mode)) {
            callback(path, FILE_TYPE_DIRECTORY, 0, NULL, NULL);
            if (increment_return_code != BUILD_SUCCESS) {
                (void)closedir(xldir);
            }
            PG_CHECKBUILD_AND_RETURN();
            /* recurse to handle subdirectories */
            recurse_dir(datadir, path, callback);
            PG_CHECKBUILD_AND_RETURN();
        } else if (S_ISLNK(fst.st_mode)) {
            char link_target[MAXPGPATH] = {'\0'};
            int len;

            len = readlink(fullpath, link_target, sizeof(link_target));
            if (len < 0) {
                pg_fatal("could not read symbolic link \"%s\": %s\n", fullpath, strerror(errno));
                (void)closedir(xldir);
                return BUILD_FATAL;
            }
            if (len >= (int)sizeof(link_target)) {
                pg_fatal("symbolic link \"%s\" target is too long\n", fullpath);
                (void)closedir(xldir);
                return BUILD_FATAL;
            }
            link_target[len] = '\0';

            callback(path, FILE_TYPE_SYMLINK, 0, link_target, NULL);

            /*
             * If it's a symlink within pg_tblspc, we need to recurse into it,
             * to process all the tablespaces.  We also follow a symlink if
             * it's for pg_xlog.  Symlinks elsewhere are ignored.
             */
            if (((parentpath != NULL) && strcmp(parentpath, "pg_tblspc") == 0) || strcmp(path, "pg_xlog") == 0) {
                recurse_dir(datadir, path, callback);
                if (increment_return_code != BUILD_SUCCESS) {
                    (void)closedir(xldir);
                }
                PG_CHECKBUILD_AND_RETURN();
            }
        }
    }

    if (errno) {
        pg_fatal("could not read directory \"%s\": %s\n", fullparentpath, strerror(errno));
        (void)closedir(xldir);
        return BUILD_FATAL;
    }

    if (closedir(xldir)) {
        pg_fatal("could not close directory \"%s\": %s\n", fullparentpath, strerror(errno));
        return BUILD_FATAL;
    }
    return BUILD_SUCCESS;
}

/* construct slotname */
void get_source_slotname(void)
{
    int rc = 0;

    Assert(conn != NULL);

    if (replication_type == RT_WITH_DUMMY_STANDBY) {
        rc = snprintf_s(source_slot_name, NAMEDATALEN, NAMEDATALEN - 1, "%s", pgxcnodename);
        securec_check_ss_c(rc, "\0", "\0");
    } else /* RT_WITH_MULTI_STANDBY */
    {
        get_slot_name_by_app_name();
    }

    return;
}

/*
 * Get the string beside space or sep
 */
static char* trim_str(char* str, int str_len, char sep)
{
    int len;
    char *begin = NULL, *end = NULL, *cur = NULL;
    char* cpyStr = NULL;
    errno_t rc;

    if (str == NULL || str_len <= 0)
        return NULL;

    cpyStr = (char*)pg_malloc(str_len);
    begin = str;
    while (begin != NULL && (isspace((int)*begin) || *begin == sep))
        begin++;

    for (end = cur = begin; *cur != '\0'; cur++) {
        if (!isspace((int)*cur) && *cur != sep)
            end = cur;
    }

    if (*begin == '\0') {
        pfree(cpyStr);
        cpyStr = NULL;
        return NULL;
    }

    len = end - begin + 1;
    rc = memmove_s(cpyStr, str_len, begin, len);
    securec_check_c(rc, "\0", "\0");
    cpyStr[len] = '\0';

    return cpyStr;
}

/*
 * get the slot name from application_name specified in postgresql.conf
 */
static void get_slot_name_by_app_name(void)
{
    const char** optlines = NULL;
    int lines_index = 0;
    int optvalue_off;
    int optvalue_len;
    char arg_str[NAMEDATALEN] = {0};
    const char* config_para_build = "application_name";
    int rc = 0;
    char conf_path[MAXPGPATH] = {0};
    char* trim_app_name = NULL;

    rc = snprintf_s(conf_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, "postgresql.conf");
    securec_check_ss_c(rc, "\0", "\0");

    if ((optlines = (const char**)readfile(conf_path)) != NULL) {
        lines_index = find_gucoption(optlines, config_para_build, NULL, NULL, &optvalue_off, &optvalue_len);

        if (lines_index != INVALID_LINES_IDX) {
            errno_t rc = 0;
            rc = strcpy_s(arg_str, NAMEDATALEN, optlines[lines_index] + optvalue_off);
            securec_check_c(rc, "", "");
        }

        /* first free one-dimensional array memory in case memory leak */
        int i = 0;
        while (optlines[i] != NULL) {
            free(const_cast<char *>(optlines[i]));
            optlines[i] = NULL;
            i++;
        }
        free(optlines);
        optlines = NULL;
    } else {
        write_stderr(_("%s cannot be opened.\n"), conf_path);
        return;
    }

    /* construct slotname */
    trim_app_name = trim_str(arg_str, NAMEDATALEN, '\'');
    if (trim_app_name != NULL) {
        rc = snprintf_s(source_slot_name, NAMEDATALEN, NAMEDATALEN - 1, "%s", trim_app_name);
        securec_check_ss_c(rc, "", "");
        free(trim_app_name);
        trim_app_name = NULL;
    }
    return;
}

/*
 * get log_directory guc specified in postgresql.conf
 */
static void get_log_directory_guc(void)
{
    const char** optlines = NULL;
    int lines_index = 0;
    int optvalue_off;
    int optvalue_len;
    const char* config_para_build = "log_directory";
    int rc = 0;
    char conf_path[MAXPGPATH] = {0};
    char* trim_name = NULL;

    rc = snprintf_s(conf_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", datadir_target, "postgresql.conf");
    securec_check_ss_c(rc, "\0", "\0");

    if ((optlines = (const char**)readfile(conf_path)) != NULL) {
        lines_index = find_gucoption(optlines, config_para_build, NULL, NULL, &optvalue_off, &optvalue_len, '\'');

        if (lines_index != INVALID_LINES_IDX) {
            rc = strncpy_s(log_directory, MAXPGPATH,
                optlines[lines_index] + optvalue_off, (size_t)Min(optvalue_len, MAX_VALUE_LEN - 1));
            securec_check_c(rc, "", "");
        } else {
            /* default log directory is pg_log under datadir */
            rc = strcpy_s(log_directory, MAXPGPATH, "pg_log");
            securec_check_c(rc, "", "");
        }

        /* first free one-dimensional array memory in case memory leak */
        int i = 0;
        while (optlines[i] != NULL) {
            free(const_cast<char *>(optlines[i]));
            optlines[i] = NULL;
            i++;
        }
        free(optlines);
        optlines = NULL;
    } else {
        write_stderr(_("%s cannot be opened.\n"), conf_path);
        return;
    }

    trim_name = trim_str(log_directory, MAXPGPATH, '\'');
    if (trim_name != NULL) {
        rc = snprintf_s(log_directory, MAXPGPATH, MAXPGPATH - 1, "%s", trim_name);
        securec_check_ss_c(rc, "", "");
        free(trim_name);
        trim_name = NULL;
    }

    pg_log(PG_WARNING, "Get log directory guc is %s\n", log_directory);

    return;
}


/*
 * Check if source DN primary is connected to dummy standby.
 *
 * If not, build will exit, because the source may have wrong xlog records without
 * replicated to dummystandby, and the target and dummystandby may have the correct
 * logs. Or else, the correct logs on target maybe lost after build started.
 */
bool checkDummyStandbyConnection(void)
{
    PGresult* res;
    char* peer_role = NULL;
    char* state = NULL;
    int rn;
    bool ret = false;

    res = PQexec(conn, "select peer_role, state from pg_stat_get_wal_senders()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_ERROR, "could not get dummystandby connection status on source: %s", PQresultErrorMessage(res));
        PQclear(res);
        res = NULL;
        return ret;
    }

    /* Read result with keyword "Secondary" and "Streaming" */
    for (rn = 0; rn < PQntuples(res); rn++) {
        peer_role = PQgetvalue(res, rn, 0);
        state = PQgetvalue(res, rn, 1);
        if (peer_role == NULL) {
            continue;
        }
        if (strcmp(peer_role, "Secondary") == 0 &&
            (strcmp(state, "Streaming") == 0 || strcmp(state, "Catchup") == 0)) {
            ret = true;
            break;
        }
        pg_log(PG_WARNING, " the results are peer_role: %s, state: %s\n", peer_role, state);
    }

    PQclear(res);
    res = NULL;
    return ret;
}

/*
 * Print a progress report based on the global variables.
 * Execute this function in another thread and print the progress periodically.
 */
static void *ProgressReportIncrementalBuild(void *arg)
{
    if (fetch_size == 0) {
        return nullptr;
    }
    char progressBar[53];
    int percent;
    do {
        /* progress report */
        percent = (int)(fetch_done * 100 / fetch_size);
        GenerateProgressBar(percent, progressBar);
        fprintf(stdout, "Progress: %s %d%% (%lu/%luKB). fetch files \r",
            progressBar, percent, fetch_done / 1024, fetch_size /1024);
        pthread_mutex_lock(&g_mutex);
        timespec timeout;
        timeval now;
        gettimeofday(&now, nullptr);
        timeout.tv_sec = now.tv_sec + 1;
        timeout.tv_nsec = 0;
        int ret = pthread_cond_timedwait(&g_cond, &g_mutex, &timeout);
        pthread_mutex_unlock(&g_mutex);
        if (ret == ETIMEDOUT) {
            continue;
        } else {
            break;
        }
    } while ((fetch_done < fetch_size) && !g_progressFlag);
    percent = 100;
    GenerateProgressBar(percent, progressBar);
    fprintf(stdout, "Progress: %s %d%% (%lu/%luKB). fetch files \n",
            progressBar, percent, fetch_done /1024, fetch_size / 1024);
    return nullptr;
}