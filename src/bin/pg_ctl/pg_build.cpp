/* -------------------------------------------------------------------------
 *
 * pg_build.c -
 *
 * Author:
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *           src/bin/pg_ctl/pg_build.c
 * -------------------------------------------------------------------------
 */
#include <dirent.h>
#include "funcapi.h"
#include <stdlib.h>
#include <stdio.h>
#include <malloc.h>

#include "backup.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pg_build.h"
#include "streamutil.h"
#include "logging.h"

#include "bin/elog.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "common/fe_memutils.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"

/* global variables for con */
char conninfo_global[MAX_REPLNODE_NUM][MAX_VALUE_LEN] = {{0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}};
static const char* config_para_build[MAX_REPLNODE_NUM] = {"",
    "replconninfo1",
    "replconninfo2",
    "replconninfo3",
    "replconninfo4",
    "replconninfo5",
    "replconninfo6",
    "replconninfo7"};

/* Node name */
char pgxcnodename[MAX_VALUE_LEN] = {0};
char config_cascade_standby[MAXPGPATH] = {0};
bool cascade_standby = false;
int replconn_num = 0;
int conn_flag = 0;
char g_buildapplication_name[MAX_VALUE_LEN] = {0};
char g_buildprimary_slotname[MAX_VALUE_LEN] = {0};
char g_str_replication_type[MAX_VALUE_LEN] = {0};
int g_replconn_idx = -1;
int g_replication_type = -1;
#define RT_WITH_DUMMY_STANDBY 0
#define RT_WITH_MULTI_STANDBY 1

int32 pg_atoi(const char* s, int size, int c)
{
    long l;
    char* badp = NULL;

    /*
     * Some versions of strtol treat the empty string as an error, but some
     * seem not to.  Make an explicit test to be sure we catch it.
     */
    if (s == NULL || *s == 0) {
        exit(1);
    }

    errno = 0;
    l = strtol(s, &badp, 10);

    /* We made no progress parsing the string, so bail out */
    if (s == badp) {
        exit(1);
    }

    switch (size) {
        case sizeof(int32):
            if (errno == ERANGE
#if defined(HAVE_LONG_INT_64)
                /* won't get ERANGE on these with 64-bit longs... */
                || l < INT_MIN || l > INT_MAX
#endif
            )
                exit(1);
            break;
        case sizeof(int16):
            if (errno == ERANGE || l < SHRT_MIN || l > SHRT_MAX)
                exit(1);
            break;
        case sizeof(int8):
            if (errno == ERANGE || l < SCHAR_MIN || l > SCHAR_MAX)
                exit(1);
            break;
        default:
            exit(1);
    }

    /*
     * Skip any trailing whitespace; if anything but whitespace remains before
     * the terminating character, bail out
     */
    while (*badp && *badp != c && isspace((unsigned char)*badp)) {
        badp++;
    }

    if (*badp && *badp != c) {
        exit(1);
    }

    return (int32)l;
}

/*
 * get the lines from a text file - return NULL if file can't be opened
 */

char** readfile(const char* path)
{
    int fd = -1;
    int nlines;
    char** result = NULL;
    char* buffer = NULL;
    char* linebegin = NULL;
    int i = 0;
    int n = 0;
    int len = 0;
    struct stat statbuf;
    errno_t rc = 0;

    /*
     * Slurp the file into memory.
     *
     * The file can change concurrently, so we read the whole file into memory
     * with a single read() call. That's not guaranteed to get an atomic
     * snapshot, but in practice, for a small file, it's close enough for the
     * current use.
     */
    fd = open(path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        return NULL;
    }
    if (fstat(fd, &statbuf) < 0) {
        close(fd);
        fd = -1;
        return NULL;
    }
    if (statbuf.st_size == 0) {
        /* empty file */
        close(fd);
        result = (char**)pg_malloc(sizeof(char*));
        *result = NULL;
        return result;
    }
    if (statbuf.st_size + 1 > MAX_CONFIG_FILE_SIZE) {
        /* empty file */
        close(fd);
        result = (char**)pg_malloc(sizeof(char*));
        *result = NULL;
        return result;
    }

    buffer = (char*)pg_malloc(statbuf.st_size + 1);

    len = read(fd, buffer, statbuf.st_size + 1);
    close(fd);
    fd = -1;
    if (len != statbuf.st_size) {
        /* oops, the file size changed between fstat and read */
        free(buffer);
        buffer = NULL;
        return NULL;
    }

    /*
     * Count newlines. We expect there to be a newline after each full line,
     * including one at the end of file. If there isn't a newline at the end,
     * any characters after the last newline will be ignored.
     */
    nlines = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            nlines++;
        }
    }

    /* set up the result buffer */
    result = (char**)pg_malloc((nlines + 1) * sizeof(char*));

    /* now split the buffer into lines */
    linebegin = buffer;
    n = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            int slen = &buffer[i] - linebegin + 1;
            char* linebuf = (char*)pg_malloc(slen + 1);
            rc = memcpy_s(linebuf, (slen + 1), linebegin, slen);
            securec_check_c(rc, "", "");
            linebuf[slen] = '\0';
            result[n++] = linebuf;
            linebegin = &buffer[i + 1];
        }
    }
    result[n] = NULL;

    free(buffer);
    buffer = NULL;

    return result;
}

/*
 * @@GaussDB@@
 * Brief            : CheckReplChannel
 * Description        :
 * Notes            :
 */
static bool CheckReplChannel(const char* ChannelInfo)
{
    char* iter = NULL;
    char* ReplStr = NULL;

    if (ChannelInfo == NULL) {
        return false;
    } else {
        ReplStr = strdup(ChannelInfo);
        if (ReplStr == NULL) {
            return false;
        } else {
            /* check localhost */
            iter = strstr(ReplStr, "localhost");
            if (iter == NULL) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }
            iter += strlen("localhost");
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if ((!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) ||
                (0 == strncmp(iter, "localport", strlen("localport")))) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }

            /* check localport */
            iter = strstr(ReplStr, "localport");
            if (iter == NULL) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }
            iter += strlen("localport");
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter)) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }

            /* check remotehost */
            iter = strstr(ReplStr, "remotehost");
            if (NULL == iter) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }
            iter += strlen("remotehost");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if ((!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) ||
                (0 == strncmp(iter, "remoteport", strlen("remoteport")))) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }

            /* check remoteport */
            iter = strstr(ReplStr, "remoteport");
            if (iter == NULL) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }
            iter += strlen("remoteport");
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter)) {
                free(ReplStr);
                ReplStr = NULL;
                return false;
            }
        }
    }

    free(ReplStr);
    ReplStr = NULL;
    return true;
}
/*
 * @@GaussDB@@
 * Brief            : GetLengthAndCheckReplConn
 * Description        :
 * Notes            :
 */
int GetLengthAndCheckReplConn(const char* ConnInfoList)
{
    int repl_len = 0;
    char* ReplStr = NULL;
    char* token = NULL;
    char* p = NULL;

    if (ConnInfoList == NULL) {
        return repl_len;
    } else {
        ReplStr = strdup(ConnInfoList);
        if (ReplStr == NULL) {
            return repl_len;
        }
        token = strtok_r(ReplStr, ",", &p);
        while (token != NULL) {
            if (CheckReplChannel(token)) {
                repl_len++;
            }

            token = strtok_r(NULL, ",", &p);
        }
    }

    free(ReplStr);
    ReplStr = NULL;
    return repl_len;
}

/*
 * @@GaussDB@@
 * Brief            : ParseReplConnInfo
 * Description        :
 * Notes            :
 */
ReplConnInfo* ParseReplConnInfo(const char* ConnInfoList, int* InfoLength)
{
    ReplConnInfo* repl = NULL;

    int repl_length = 0;
    char* iter = NULL;
    char* pNext = NULL;
    char* ReplStr = NULL;
    char* token = NULL;
    char* ptr = NULL;
    int parsed = 0;
    int iplen = 0;
    char tmp_localhost[IP_LEN] = {0};
    int tmp_localport = 0;
    int tmp_localservice = 0;
    char tmp_remotehost[IP_LEN] = {0};
    int tmp_remoteport = 0;
    int tmp_remoteservice = 0;
    errno_t rc = EOK;
    char* p = NULL;

    if (ConnInfoList == NULL) {
        return NULL;
    } else {
        ReplStr = strdup(ConnInfoList);
        if (ReplStr == NULL) {
            return NULL;
        }

        ptr = ReplStr;
        while (*ptr != '\0') {
            if (*ptr != ' ') {
                break;
            }
            ptr++;
        }
        if (*ptr == '\0') {
            free(ReplStr);
            ReplStr = NULL;
            return NULL;
        }

        repl_length = GetLengthAndCheckReplConn(ReplStr);
        if (repl_length == 0) {
            free(ReplStr);
            ReplStr = NULL;
            return NULL;
        }

        repl = (ReplConnInfo*)malloc(sizeof(ReplConnInfo));
        if (repl == NULL) {
            free(ReplStr);
            ReplStr = NULL;
            return NULL;
        }
        rc = memset_s(repl, sizeof(ReplConnInfo), 0, sizeof(ReplConnInfo));
        securec_check_c(rc, "", "");

        token = strtok_r(ReplStr, ",", &p);
        while (token != NULL) {
            rc = memset_s(tmp_localhost, IP_LEN, 0, sizeof(tmp_localhost));
            securec_check_c(rc, "\0", "\0");

            rc = memset_s(tmp_remotehost, IP_LEN, 0, sizeof(tmp_remotehost));
            securec_check_c(rc, "\0", "\0");

            /* localhost */
            iter = strstr(token, "localhost");
            if (iter == NULL) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            iter += strlen("localhost");
            while (' ' == *iter || '=' == *iter) {
                iter++;
            }
            if (!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            pNext = iter;
            iplen = 0;
            while (*pNext != ' ' && 0 != strncmp(pNext, "localport", strlen("localport"))) {
                iplen++;
                pNext++;
            }

            rc = strncpy_s(tmp_localhost, IP_LEN, iter, iplen);
            securec_check_c(rc, "", "");

            tmp_localhost[IP_LEN - 1] = '\0';

            /* localport */
            iter = strstr(token, "localport");
            if (iter == NULL) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            iter += strlen("localport");
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter)) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            tmp_localport = atoi(iter);

            /* localservice */
            iter = strstr(token, "localservice");
            if (iter != NULL) {
                iter += strlen("localservice");
                ;
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }

                if (isdigit(*iter)) {
                    tmp_localservice = atoi(iter);
                }
            }

            /* remotehost */
            iter = strstr(token, "remotehost");
            if (iter == NULL) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            iter += strlen("remotehost");
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter) && *iter != ':' && !isalpha(*iter)) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            pNext = iter;
            iplen = 0;
            while (*pNext != ' ' && 0 != strncmp(pNext, "remoteport", strlen("remoteport"))) {
                iplen++;
                pNext++;
            }
            rc = strncpy_s(tmp_remotehost, IP_LEN, iter, iplen);
            securec_check_c(rc, "", "");

            tmp_remotehost[IP_LEN - 1] = '\0';

            /* remoteport */
            iter = strstr(token, "remoteport");
            if (NULL == iter) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            iter += strlen("remoteport");
            while (*iter == ' ' || *iter == '=') {
                iter++;
            }
            if (!isdigit(*iter)) {
                token = strtok_r(NULL, ",", &p);
                continue;
            }
            tmp_remoteport = atoi(iter);

            /* remoteservice */
            iter = strstr(token, "remoteservice");
            if (NULL != iter) {
                iter += strlen("remoteservice");
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }

                if (isdigit(*iter)) {
                    tmp_remoteservice = atoi(iter);
                }
            }

            /* copy the valus from tmp */
            rc = strncpy_s(repl->localhost, IP_LEN, tmp_localhost, IP_LEN - 1);
            securec_check_c(rc, "", "");

            repl->localhost[IP_LEN - 1] = '\0';
            repl->localport = tmp_localport;
            repl->localservice = tmp_localservice;

            rc = strncpy_s(repl->remotehost, IP_LEN, tmp_remotehost, IP_LEN - 1);
            securec_check_c(rc, "", "");

            repl->remotehost[IP_LEN - 1] = '\0';
            repl->remoteport = tmp_remoteport;
            repl->remoteservice = tmp_remoteservice;

            token = strtok_r(NULL, ",", &p);
            parsed++;
        }
    }

    free(ReplStr);
    ReplStr = NULL;
    *InfoLength = repl_length;

    return repl;
}

/*
 * Brief            : @@GaussDB@@
 * Description        : get available conn
 * Notes            :
 */
void get_conninfo(const char* filename)
{
    char** optlines;
    int lines_index = 0;
    int optvalue_off;
    int optvalue_len;
    int opt_index = 0;
    errno_t rc = EOK;

    if (filename == NULL) {
        pg_log(PG_PRINT, _("filename is NULL"));
        exit(1);
    }

    /**********************************************************
    Try to read the config file
    ************************************************************/
    if ((optlines = readfile(filename)) != NULL) {
        int i;

        /* get connection str from commands,different from DN Standby connect to DN Primary */
        if (NULL != conn_str) {
            size_t conn_len = (size_t)(strlen(conn_str) + 1);
            rc = strncpy_s(conninfo_global[0],
                MAX_VALUE_LEN,
                conn_str,
                (conn_len >= MAX_VALUE_LEN ? MAX_VALUE_LEN - 2 : conn_len - 1));
            securec_check_c(rc, "", "");
        } else {
            /* read repconninfo[...] */
            for (i = 1; i < MAX_REPLNODE_NUM; i++) {
                lines_index = find_gucoption((const char**)optlines,
                    (const char*)config_para_build[i],
                    NULL,
                    NULL,
                    &optvalue_off,
                    &optvalue_len);

                if (lines_index != INVALID_LINES_IDX) {
                    rc = strncpy_s(conninfo_global[i - 1],
                        MAX_VALUE_LEN,
                        optlines[lines_index] + optvalue_off + 1,
                        (size_t)Min(optvalue_len - 2, MAX_VALUE_LEN - 1));
                    securec_check_c(rc, "", "");
                }
            }
        }
        /* read cascade standby */
        lines_index =
            find_gucoption((const char**)optlines, CONFIG_CASCADE_STANDBY, NULL, NULL, &optvalue_off, &optvalue_len);
        if (lines_index == INVALID_LINES_IDX) {
            cascade_standby = false;
        }
#ifndef ENABLE_LLT
        else {
            rc = strncpy_s(config_cascade_standby,
                MAXPGPATH,
                optlines[lines_index] + optvalue_off,
                (size_t)Min(optvalue_len, MAX_VALUE_LEN - 1));
            securec_check_c(rc, "", "");
            if (0 == strcmp(config_cascade_standby, "on")) {
                cascade_standby = true;
            } else if (0 == strcmp(config_cascade_standby, "off")) {
                cascade_standby = false;
            }
        }
#endif
        if (g_buildapplication_name != NULL && strlen(g_buildapplication_name) <= 0) {
            lines_index =
                find_gucoption((const char**)optlines, "application_name", NULL, NULL, &optvalue_off, &optvalue_len);
            if (lines_index != INVALID_LINES_IDX) {
                rc = strncpy_s(g_buildapplication_name,
                    MAX_VALUE_LEN,
                    optlines[lines_index] + optvalue_off + 1,
                    (size_t)Min(optvalue_len - 2, MAX_VALUE_LEN - 1));
                securec_check_c(rc, "", "");
            }
        }

        if (g_replication_type == -1) {
            lines_index =
                find_gucoption((const char**)optlines, "replication_type", NULL, NULL, &optvalue_off, &optvalue_len);

            if (lines_index != INVALID_LINES_IDX) {
                rc = strncpy_s(g_str_replication_type,
                    MAX_VALUE_LEN,
                    optlines[lines_index] + optvalue_off,
                    (size_t)Min(optvalue_len, MAX_VALUE_LEN - 1));
                securec_check_c(rc, "", "");
                g_str_replication_type[1] = 0;
                g_replication_type = atoi(g_str_replication_type);
                if(g_replication_type != RT_WITH_DUMMY_STANDBY && g_replication_type != RT_WITH_MULTI_STANDBY) {
                     write_stderr(_("replication_type invalid.\n"));
                     exit(1);
                }
            } else {
                /* set as default value */
                g_replication_type = RT_WITH_DUMMY_STANDBY;
            }
        }

        if (g_buildprimary_slotname != NULL && strlen(g_buildprimary_slotname) <= 0) {
            lines_index =
                find_gucoption((const char**)optlines, "primary_slotname", NULL, NULL, &optvalue_off, &optvalue_len);
            if (lines_index != INVALID_LINES_IDX) {
                rc = strncpy_s(g_buildprimary_slotname,
                    MAX_VALUE_LEN,
                    optlines[lines_index] + optvalue_off + 1,
                    (size_t)Min(optvalue_len - 2, MAX_VALUE_LEN - 1));
                securec_check_c(rc, "", "");
            }
        }
        /* read pgxc_node */
        lines_index = find_gucoption((const char**)optlines, CONFIG_NODENAME, NULL, NULL, &optvalue_off, &optvalue_len);
        if (lines_index != INVALID_LINES_IDX) {
            rc = strncpy_s(pgxcnodename,
                MAX_VALUE_LEN,
                optlines[lines_index] + optvalue_off + 1,
                (size_t)Min(optvalue_len - 2, MAX_VALUE_LEN - 1));
            securec_check_c(rc, "", "");
        }

        while (optlines[opt_index] != NULL) {
            free(optlines[opt_index]);
            optlines[opt_index] = NULL;
            opt_index++;
        }

        free(optlines);
        optlines = NULL;
    } else {
        pg_log(PG_PRINT, _("%s cannot be opened.\n"), filename);
        exit(1);
    }
}

#define disconnect_and_return_null(tempconn) \
    do {                                     \
        if ((tempconn) != NULL) {            \
            PQfinish(tempconn);              \
            tempconn = NULL;                 \
        }                                    \
        tempconn = NULL;                     \
        return tempconn;                     \
    } while (0)

/*
 * Brief            : @@GaussDB@@
 * Description        : compare local version and protocal version with remote server.
 * Notes            :
 */
static bool check_remote_version(PGconn* conn_get, uint32 term)
{
    PGresult* res = NULL;
    int primary_sversion = 0;
    char* primary_pversion = NULL;
    bool version_match = false;
    uint32 primary_term;
    Assert(conn_get != NULL);

    res = PQexec(conn_get, "IDENTIFY_VERSION");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        goto exit;
    }
    if (PQnfields(res) != 3 || PQntuples(res) != 1) {
        goto exit;
    }

    primary_sversion = pg_atoi((const char*)PQgetvalue(res, 0, 0), 4, 0);
    primary_pversion = PQgetvalue(res, 0, 1);
    primary_term = pg_atoi((const char*)PQgetvalue(res, 0, 2), 4, 0);
    if (term > primary_term) {
        pg_log(PG_PRINT, "the primary term %u is smaller than the standby term %u.\n", primary_term, term);
        goto exit;
    }
    if (primary_sversion != PG_VERSION_NUM ||
        strncmp(primary_pversion, PG_PROTOCOL_VERSION, strlen(PG_PROTOCOL_VERSION)) != 0) {
        if (primary_sversion != PG_VERSION_NUM) {
            pg_log(PG_PRINT,
                "%s: database system version is different between the primary and standby "
                "The primary's system version is %d, the standby's system version is %d.\n",
                progname,
                primary_sversion,
                PG_VERSION_NUM);
        } else {
            pg_log(PG_PRINT,
                "%s: the primary protocal version %s is not the same as the standby protocal version %s.\n",
                progname,
                primary_pversion,
                PG_PROTOCOL_VERSION);
        }
        goto exit;
    }

    version_match = true;

exit:
    PQclear(res);
    return version_match;
}

/*
 * Brief            : @@GaussDB@@
 * Description        : get remote server's mode
 * Notes            :
 */
static ServerMode get_remote_mode(PGconn* conn_get)
{
    PGresult* res = NULL;
    ServerMode primary_mode = UNKNOWN_MODE;

    Assert(conn_get != NULL);

    res = PQexec(conn_get, "IDENTIFY_MODE");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        goto exit;
    }
    if (PQnfields(res) != 1 || PQntuples(res) != 1) {
        goto exit;
    }
    primary_mode = (ServerMode)pg_atoi((const char*)PQgetvalue(res, 0, 0), 4, 0);

exit:
    PQclear(res);

    /* conn_get alse alive, shoule release it outside. */
    return primary_mode;
}

/*
 * Brief            : @@GaussDB@@
 * Description        : connect the server by connstr and
 *                       check whether the remote server is primary,
 *                          return conn if success
 * Notes            :
 */
static PGconn* check_and_get_primary_conn(const char* repl_conninfo, uint32 term)
{
    PGconn* conn_get = NULL;
    ServerMode remote_mode = UNKNOWN_MODE;

    Assert(repl_conninfo != NULL);

    /* 1. Connect server */
    conn_get = PQconnectdb(repl_conninfo);
    if ((conn_get == NULL) || PQstatus(conn_get) != CONNECTION_OK) {
        disconnect_and_return_null(conn_get);
    }

    /* 2. IDENTIFY_VERSION */
    if (!check_remote_version(conn_get, term)) {
        disconnect_and_return_null(conn_get);
    }

    /* 3. IDENTIFY_MODE */
    remote_mode = get_remote_mode(conn_get);
    if (remote_mode != NORMAL_MODE && remote_mode != PRIMARY_MODE) {
        disconnect_and_return_null(conn_get);
    }

    /* here we get the right primary connect */
    return conn_get;
}

PGconn* check_and_conn(int conn_timeout, int recv_timeout, uint32 term)
{
    PGconn* con_get = NULL;
    char repl_conninfo_str[MAXPGPATH];
    int tnRet = 0;
    int repl_arr_length;
    int i = 0;
    int parse_failed_num = 0;

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        ReplConnInfo* repl_conn_info = ParseReplConnInfo(conninfo_global[i - 1], &repl_arr_length);
        if (repl_conn_info == NULL) {
            parse_failed_num++;
            continue;
        }

        tnRet = memset_s(repl_conninfo_str, MAXPGPATH, 0, MAXPGPATH);
        securec_check_ss_c(tnRet, "", "");

        tnRet = snprintf_s(repl_conninfo_str,
            sizeof(repl_conninfo_str),
            sizeof(repl_conninfo_str) - 1,
            "localhost=%s localport=%d host=%s port=%d "
            "dbname=replication replication=true "
            "fallback_application_name=gs_ctl "
            "connect_timeout=%d rw_timeout=%d "
            "options='-c remotetype=application'",
            repl_conn_info->localhost,
            repl_conn_info->localport,
            repl_conn_info->remotehost,
            repl_conn_info->remoteport,
            conn_timeout,
            recv_timeout);
        securec_check_ss_c(tnRet, "", "");

        free(repl_conn_info);
        repl_conn_info = NULL;
        con_get = check_and_get_primary_conn(repl_conninfo_str, term);
        if (con_get != NULL)
            break;
    }

    if (parse_failed_num == MAX_REPLNODE_NUM - 1) {
        pg_log(PG_WARNING, " invalid value for parameter \"replconninfo\" in postgresql.conf.\n");
        if (con_get != NULL)
            PQfinish(con_get);

        exit(1);
    }

    return con_get;
}

/*
 * Brief            : @@GaussDB@@
 * Description    : find the value of guc para according to name
 * Notes            :
 */
int find_gucoption(
    const char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len)
{
    const char* p = NULL;
    const char* q = NULL;
    const char* tmp = NULL;
    int i = 0;
    size_t paramlen = 0;

    if (optlines == NULL || opt_name == NULL) {
        return INVALID_LINES_IDX;
    }
    paramlen = (size_t)strnlen(opt_name, MAX_PARAM_LEN);
    if (name_len != NULL) {
        *name_len = (int)paramlen;
    }
    for (i = 0; optlines[i] != NULL; i++) {
        p = optlines[i];
        while (isspace((unsigned char)*p)) {
            p++;
        }
        if (strncmp(p, opt_name, paramlen) != 0) {
            continue;
        }
        if (name_offset != NULL) {
            *name_offset = p - optlines[i];
        }
        p += paramlen;
        while (isspace((unsigned char)*p)) {
            p++;
        }
        if (*p != '=') {
            continue;
        }
        p++;
        while (isspace((unsigned char)*p)) {
            p++;
        }
        q = p;
        while (*q && !(*q == '\n' || *q == '#')) {
            if (!isspace((unsigned char)*q)) {
                tmp = ++q;
            } else {
                q++;
            }
        }
        if (value_offset != NULL) {
            *value_offset = p - optlines[i];
        }
        if (value_len != NULL) {
            *value_len = (tmp == NULL) ? 0 : (tmp - p);
        }
        return i;
    }

    return INVALID_LINES_IDX;
}

/*
 * Brief            : @@GaussDB@@
 * Description    :  delete data/ and pg_tblspc/
 * Notes            :
 */
void delete_datadir(const char* dirname)
{
    DIR* dir = NULL;
    char fullpath[MAXPGPATH] = {0};
    char nodepath[MAXPGPATH] = {0};
    char xlogpath[MAXPGPATH] = {0};
    struct dirent* de = NULL;
    struct stat st;
    struct stat stbuf;

    errno_t rc = 0;
    int nRet = 0;

    if (dirname == NULL) {
        pg_log(PG_WARNING, _("input parameter is NULL.\n"));
        exit(1);
    }
    nRet = snprintf_s(fullpath, MAXPGPATH, sizeof(fullpath) - 1, "%s/pg_tblspc", dirname);
    securec_check_ss_c(nRet, "", "");

    /* pg_tblspc */
    if ((dir = opendir(fullpath)) != NULL) {
        while ((de = gs_readdir(dir)) != NULL) {
            char linkpath[MAXPGPATH] = {0};
            int rllen = 0;
            /* Skip special stuff */
            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                continue;
            rc = memset_s(fullpath, MAXPGPATH, 0, MAXPGPATH);
            securec_check_c(rc, "", "");
            nRet = snprintf_s(fullpath, MAXPGPATH, sizeof(fullpath) - 1, "%s/pg_tblspc/%s", dirname, de->d_name);
            securec_check_ss_c(nRet, "", "");

#if defined(HAVE_READLINK) || defined(WIN32)

            rllen = readlink(fullpath, linkpath, sizeof(linkpath));
            if (rllen < 0) {
                pg_log(PG_WARNING, _("could not read symbolic link.\n"));
                continue;
            }
            if (rllen >= (int)sizeof(linkpath)) {
                pg_log(PG_WARNING, _("symbolic link target is too long.\n"));
                continue;
            }
            linkpath[MAXPGPATH - 1] = '\0';

            /*  */
            if (stat(linkpath, &st) == 0 && S_ISDIR(st.st_mode)) {

                nRet = snprintf_s(nodepath,
                    MAXPGPATH,
                    sizeof(nodepath) - 1,
                    "%s/%s_%s",
                    linkpath,
                    TABLESPACE_VERSION_DIRECTORY,
                    pgxcnodename);
                securec_check_ss_c(nRet, "", "");

                if (!rmtree(nodepath, true, true)) {
                    pg_log(PG_WARNING, _("failed to remove %s.\n"), nodepath);
                    (void)closedir(dir);
                    exit(1);
                }

                /* just try to delete the top folder regardless of success or failure. */
                (void)unlink(linkpath);
            }

            /* remove link file. */
            if (unlink(fullpath) < 0) {
                pg_log(PG_WARNING, _("could not remove symbolic link file \"%s\": %s\n"), fullpath, strerror(errno));
                (void)closedir(dir);
                exit(1);
            }
#else
            /*
             * If the platform does not have symbolic links, it should not be
             * possible to have tablespaces - clearly somebody else created
             * them. Warn about it and ignore.
             */
            pg_log(PG_WARNING, _("symbolic links are not supported on this platform.\n"));
            exit(1);
#endif
        }
        (void)closedir(dir);
    }

    /*
     * in build process, primary node send string
     * 1. 'data/pg_xlog'(primary pg_xlog not symbolic) or
     * 2. pg_xlog linktarget(primary pg_xlog is symbolic)
     * to standby. case 1, standby set xlog_location to basedir/pg_xlog,
     * case 2, standby set xlog_location to primary linktarget directory.
     *
     * Enable user define xlog directory, datanode xlog directory is the value
     * passed by -X(if set), and not depend on primary anymore. But in the build
     * process, standby can't get the xlog directory info. the way to resolve
     * this is to keep the basedir/pg_xlog, and delete all files and
     * directories under it.
     */
    nRet = snprintf_s(xlogpath, MAXPGPATH, sizeof(xlogpath) - 1, "%s/pg_xlog", dirname);
    securec_check_ss_c(nRet, "", "");

    if (lstat(xlogpath, &stbuf) == 0) {
#ifndef WIN32
        if (S_ISLNK(stbuf.st_mode))
#else
        if (pgwin32_is_junction(xlogpath))
#endif
        {
#if defined(HAVE_READLINK) || defined(WIN32)
            char linkpath[MAXPGPATH] = {0};
            int rllen;

            rllen = readlink(xlogpath, linkpath, sizeof(linkpath));
            if (rllen < 0) {
                pg_log(PG_WARNING, _("could not read symbolic link.\n"));
            }
            if (rllen >= (int)sizeof(linkpath)) {
                pg_log(PG_WARNING, _("symbolic link target is too long.\n"));
            }
            linkpath[MAXPGPATH - 1] = '\0';

            /* delete targets under linktarget, but keep itself */
            if (!rmtree(linkpath, false)) {
                pg_log(PG_WARNING, _("failed to remove dir %s.\n"), linkpath);
                exit(1);
            }
#else
            /*
             * If the platform does not have symbolic links, it should not be
             * possible to have tablespaces - clearly somebody else created
             * them. Warn about it and ignore.
             */
            pg_log(PG_WARNING, _("symbolic links are not supported on this platform.\n"));
            exit(1);
#endif /* HAVE_READLINK */
        } else {
            /* delete targets under pg_xlog and itself */
            if (!rmtree(xlogpath, true)) {
                pg_log(PG_WARNING, _("failed to remove dir %s.\n"), xlogpath);
                exit(1);
            }
        }
    }

    /* data dir */
    if ((dir = opendir(dirname)) != NULL) {
        while ((de = gs_readdir(dir)) != NULL) {
            /* Skip special stuff */
            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                continue;
            if (strcmp(de->d_name, "pg_log") == 0 || strcmp(de->d_name, "pg_location") == 0)
                continue;
            if (strcmp(de->d_name, "full_upgrade_bak") == 0)
                continue;
            if (strcmp(de->d_name, "pg_xlog") == 0)
                continue;
            rc = memset_s(fullpath, MAXPGPATH, 0, MAXPGPATH);
            securec_check_c(rc, "", "");
            /* others */
            nRet = snprintf_s(fullpath, MAXPGPATH, sizeof(fullpath) - 1, "%s/%s", dirname, de->d_name);
            securec_check_ss_c(nRet, "", "");

            if (lstat(fullpath, &st) != 0) {
                pg_log(PG_WARNING, _("could not stat file or directory %s.\n"), fullpath);
                continue;
            }

#ifndef WIN32
            if (S_ISLNK(st.st_mode))
#else
            if (pgwin32_is_junction(fullpath))
#endif
            {
#if defined(HAVE_READLINK) || defined(WIN32)
                char linkpath[MAXPGPATH] = {0};
                int rllen;

                rllen = readlink(fullpath, linkpath, sizeof(linkpath));
                if (rllen < 0) {
                    pg_log(PG_WARNING, _("could not read symbolic link.\n"));
                    continue;
                }
                if (rllen >= (int)sizeof(linkpath)) {
                    pg_log(PG_WARNING, _("symbolic link target is too long.\n"));
                    continue;
                }
                linkpath[MAXPGPATH - 1] = '\0';

                /* delete linktarget */
                if (!rmtree(linkpath, true)) {
                    pg_log(PG_WARNING, _("failed to remove dir %s.\n"), linkpath);
                    (void)closedir(dir);
                    exit(1);
                }

                /* delete link */
                (void)unlink(fullpath);
#else
                /*
                 * If the platform does not have symbolic links, it should not be
                 * possible to have tablespaces - clearly somebody else created
                 * them. Warn about it and ignore.
                 */
                pg_log(PG_WARNING, _("symbolic links are not supported on this platform.\n"));
                (void)closedir(dir);
                exit(1);
#endif /* HAVE_READLINK */
            } else if (S_ISDIR(st.st_mode)) {
                if (strcmp(de->d_name, "pg_replslot") == 0) {
                    /* remove physical slot and remain logic slot */
                    DIR* dir_slot = NULL;
                    struct dirent* de_slot = NULL;
                    if ((dir_slot = opendir(fullpath)) != NULL) {
                        while ((de_slot = gs_readdir(dir_slot)) != NULL) {
                            /* Skip special stuff */
                            if (strncmp(de_slot->d_name, ".", 1) == 0 || strncmp(de_slot->d_name, "..", 2) == 0)
                                continue;

                            /* others */
                            nRet = snprintf_s(fullpath,
                                MAXPGPATH,
                                sizeof(fullpath) - 1,
                                "%s/%s/%s",
                                dirname,
                                "pg_replslot",
                                de_slot->d_name);
                            securec_check_ss_c(nRet, "", "");
                            if (!rmtree(fullpath, true)) {
                                pg_log(PG_WARNING, _("failed to remove dir %s,errno=%d.\n"), fullpath, errno);
                                (void)closedir(dir);
                                exit(1);
                            }
                        }
                        (void)closedir(dir_slot);
                    }
                } else if (!rmtree(fullpath, true)) {
                    pg_log(PG_WARNING, _("failed to remove dir %s, errno=%d.\n"), fullpath, errno);
                    (void)closedir(dir);
                    exit(1);
                }
            } else if (S_ISREG(st.st_mode)) {
                if (strcmp(de->d_name, "postgresql.conf") == 0 || strcmp(de->d_name, "pg_ctl.lock") == 0 ||
                    strcmp(de->d_name, "postgresql.conf.lock") == 0 || strcmp(de->d_name, "postgresql.conf.bak.old") == 0 ||
                    strcmp(de->d_name, "build_completed.start") == 0 || strcmp(de->d_name, "gs_build.pid") == 0 ||
                    strcmp(de->d_name, "postmaster.opts") == 0 || strcmp(de->d_name, "gaussdb.state") == 0 ||
                    strcmp(de->d_name, "disc_readonly_test") == 0 || strcmp(de->d_name, ssl_cert_file) == 0 ||
                    strcmp(de->d_name, ssl_key_file) == 0 || strcmp(de->d_name, ssl_ca_file) == 0 ||
                    strcmp(de->d_name, ssl_crl_file) == 0 || strcmp(de->d_name, ssl_cipher_file) == 0 ||
                    strcmp(de->d_name, ssl_rand_file) == 0 || strcmp(de->d_name, "rewind_lable") == 0)
                    continue;
                /* build from cn reserve this file,om will modify it */
                if ((conn_str != NULL) && 0 == strncmp(de->d_name, "pg_hba.conf", strlen("pg_hba.conf")))
                    continue;
                if (unlink(fullpath)) {
                    pg_log(PG_WARNING, _("failed to remove file %s.\n"), fullpath);
                    (void)closedir(dir);
                    exit(1);
                }
            }
        }
        (void)closedir(dir);
    }
}

/*
 * Brief            : @@GaussDB@@
 * Description        : get  the number of replication in postgresql.conf
 * Notes            :
 */
int get_replconn_number(const char* filename)
{
    char** optlines;
    int repl_num = 0;
    int opt_index = 0;

    if (filename == NULL) {
        pg_log(PG_PRINT, _("the parameter filename is NULL in function get_replconn_number()"));
        exit(1);
    }

    if ((optlines = readfile(filename)) != NULL) {
        int optvalue_off = 0;
        int optvalue_len = 0;
        int lines_index = 0;
        int i;
        for (i = 1; i < MAX_REPLNODE_NUM; i++) {
            lines_index = find_gucoption(
                (const char**)optlines, (const char*)config_para_build[i], NULL, NULL, &optvalue_off, &optvalue_len);

            if (lines_index != INVALID_LINES_IDX) {
                repl_num++;
            }
        }

        while (optlines[opt_index] != NULL) {
            free(optlines[opt_index]);
            optlines[opt_index] = NULL;
            opt_index++;
        }

        free(optlines);
        optlines = NULL;
    } else {
        pg_log(PG_PRINT, _("%s cannot be opened.\n"), filename);
        exit(1);
    }

    return repl_num;
}

/*
 * Brief            : @@GaussDB@@
 * Description    : to connect the server,and return conn if success
 * Notes            :
 */
static PGconn* get_conn(const char* repl_conninfo)
{
    PGconn* conn_get = NULL;
    PGresult* res = NULL;
    int primary_sversion = 0;
    int standby_sversion = 0;
    char* primary_pversion = NULL;
    char* standby_pversion = NULL;
    ServerMode primary_mode;

    /*  to connect server */
    conn_get = PQconnectdb(repl_conninfo);
    if ((!conn_get) || (PQstatus(conn_get) != CONNECTION_OK)) {
        disconnect_and_return_null(conn_get);
    }

    /* IDENTIFY_VERSION */
    res = PQexec(conn_get, "IDENTIFY_VERSION");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        disconnect_and_return_null(conn_get);
    }
    if (PQnfields(res) != 3 || PQntuples(res) != 1) {
        PQclear(res);
        disconnect_and_return_null(conn_get);
    }
    primary_sversion = pg_atoi((const char*)PQgetvalue(res, 0, 0), 4, 0);
    standby_sversion = PG_VERSION_NUM;
    primary_pversion = PQgetvalue(res, 0, 1);
    standby_pversion = strdup(PG_PROTOCOL_VERSION);
    if (standby_pversion == NULL) {
        PQclear(res);
        disconnect_and_return_null(conn_get);
    }

    if (primary_sversion != standby_sversion ||
        strncmp(primary_pversion, standby_pversion, strlen(PG_PROTOCOL_VERSION)) != 0) {
        PQclear(res);

        if (primary_sversion != standby_sversion) {
            pg_log(PG_PRINT,
                "%s: database system version is different between the primary and standby "
                "The primary's system version is %d, the standby's system version is %d.\n",
                progname,
                primary_sversion,
                standby_sversion);
            free(standby_pversion);
            standby_pversion = NULL;
            disconnect_and_return_null(conn_get);
        } else {
            pg_log(PG_PRINT,
                "%s: the primary protocal version %s is not the same as the standby protocal version %s.\n",
                progname,
                primary_pversion,
                standby_pversion);
            free(standby_pversion);
            standby_pversion = NULL;
            disconnect_and_return_null(conn_get);
        }
    }
    PQclear(res);

    /* free immediately once not used. Can't be NULL. */
    free(standby_pversion);
    standby_pversion = NULL;
    /* IDENTIFY_MODE */
    res = PQexec(conn_get, "IDENTIFY_MODE");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        disconnect_and_return_null(conn_get);
    }
    if (PQnfields(res) != 1 || PQntuples(res) != 1) {
        PQclear(res);
        disconnect_and_return_null(conn_get);
    }
    primary_mode = (ServerMode)pg_atoi((const char*)PQgetvalue(res, 0, 0), 4, 0);
    if (!((primary_mode == PRIMARY_MODE && cascade_standby == false) ||
            (primary_mode == STANDBY_MODE && cascade_standby == true) || (primary_mode == NORMAL_MODE))) {
        PQclear(res);
        disconnect_and_return_null(conn_get);
    }
    PQclear(res);
    return conn_get;
}

bool check_conn(int conn_timeout, int recv_timeout)
{

    ReplConnInfo* repl_conn_info = NULL;

    PGconn* con_get = NULL;
    char repl_conninfo[MAXPGPATH];
    int repl_array_length = 0;
    int i = 0;
    bool ret = false;

    /* parse conninfo */
    repl_conn_info = ParseReplConnInfo(conninfo_global[0], &repl_array_length);
    if (repl_conn_info == NULL) {
        pg_log(PG_PRINT, "%s: invalid value for parameter \"replconninfo1\" in postgresql.conf.\n", progname);
        return ret;
    }

    /* check if we can get the connection. */
    for (i = 0; i < repl_array_length; i++) {
        if (repl_conn_info != NULL) {
            int nRet = snprintf_s(repl_conninfo,
                sizeof(repl_conninfo),
                sizeof(repl_conninfo) - 1,
                "localhost=%s localport=%d host=%s port=%d "
                "dbname=replication replication=true "
                "fallback_application_name=gs_ctl "
                "connect_timeout=%d rw_timeout=%d ",
                repl_conn_info[i].localhost,
                repl_conn_info[i].localport,
                repl_conn_info[i].remotehost,
                repl_conn_info[i].remoteport,
                conn_timeout,
                recv_timeout);
            securec_check_ss_c(nRet, "", "");

            con_get = get_conn(repl_conninfo);
            if (con_get != NULL) {
                PQfinish(con_get);
                con_get = NULL;
                ret = true;
                break;
            }
        }
    }

    if (repl_conn_info != NULL) {
        free(repl_conn_info);
        repl_conn_info = NULL;
    }

    return ret;
}

void get_slot_name(char* slotname, size_t len)
{
    int errorno = memset_s(slotname, len, 0, len);
    securec_check_ss_c(errorno, "", "");
    if (g_buildprimary_slotname != NULL && strlen(g_buildprimary_slotname) > 0) {
        errorno = snprintf_s(slotname, len, len - 1, "%s", g_buildprimary_slotname);
        securec_check_ss_c(errorno, "", "");
    } else if (g_buildapplication_name != NULL && strlen(g_buildapplication_name) > 0) {
        errorno = snprintf_s(slotname, len, len - 1, "%s", g_buildapplication_name);
        securec_check_ss_c(errorno, "", "");
    } else if (pgxcnodename != NULL && strlen(pgxcnodename) > 0) {
        if(g_replication_type == RT_WITH_DUMMY_STANDBY) {
            errorno = snprintf_s(
                slotname, len, len - 1, "%s", pgxcnodename);
                securec_check_ss_c(errorno, "", "");
        } else if(g_replconn_idx != -1) {
            ReplConnInfo* repl_conn_info = NULL;
            int repl_arr_length = 0;
            repl_conn_info = ParseReplConnInfo(conninfo_global[g_replconn_idx], &repl_arr_length);
            if (repl_conn_info != NULL) {
                errorno = snprintf_s(slotname, len, len - 1, "%s_%s_%d", pgxcnodename, repl_conn_info->localhost, 
                    repl_conn_info->localport);
                securec_check_ss_c(errorno, "", "");
                free(repl_conn_info);
                repl_conn_info = NULL;
            }
        }
    }
    return;
}

/*
 * rotate cbm force when build.
 */
bool libpqRotateCbmFile(PGconn* connObj, XLogRecPtr lsn)
{
    PGresult* res = NULL;
    char sql[MAX_QUERY_LEN] = {0};
    errno_t errorno = EOK;
    bool ec = true;

    errorno = snprintf_s(sql, MAX_QUERY_LEN, MAX_QUERY_LEN - 1,
        "select * from pg_cbm_rotate_file('%08X/%08X'); ",
        (uint32)(lsn >> 32), (uint32)(lsn));
    securec_check_ss_c(errorno, "\0", "\0");
    res = PQexec(connObj, sql);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        pg_log(PG_ERROR, "could not rotate cbm in 10min: %s", PQresultErrorMessage(res));
        ec = false;
    }
    PQclear(res);
    return ec;
}

