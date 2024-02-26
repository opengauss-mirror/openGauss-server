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
#include "tool_common.h"

#include "bin/elog.h"
#include "nodes/pg_list.h"
#include "replication/replicainternal.h"
#include "storage/smgr/fd.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "common/fe_memutils.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "storage/file/fio_device.h"

/* global variables for con */
char conninfo_global[MAX_REPLNODE_NUM][MAX_VALUE_LEN] = {{0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}, {0}};
static const char* config_para_build[MAX_REPLNODE_NUM] = {"",
    "replconninfo1",
    "replconninfo2",
    "replconninfo3",
    "replconninfo4",
    "replconninfo5",
    "replconninfo6",
    "replconninfo7",
    "replconninfo8"};
static const char* config_para_cross_cluster_build[MAX_REPLNODE_NUM] = {
    "",
    "cross_cluster_replconninfo1",
    "cross_cluster_replconninfo2",
    "cross_cluster_replconninfo3",
    "cross_cluster_replconninfo4",
    "cross_cluster_replconninfo5",
    "cross_cluster_replconninfo6",
    "cross_cluster_replconninfo7",
    "cross_cluster_replconninfo8"
};

/* Node name */
char pgxcnodename[MAX_VALUE_LEN] = {0};
char config_cascade_standby[MAXPGPATH] = {0};
bool cascade_standby = false;
int replconn_num = 0;
int conn_flag = 0;
char g_buildapplication_name[MAX_VALUE_LEN] = {0};
char g_buildprimary_slotname[MAX_VALUE_LEN] = {0};
char g_str_replication_type[MAX_VALUE_LEN] = {0};
char g_repl_auth_mode[MAX_VALUE_LEN] = {0};
char g_repl_uuid[MAX_VALUE_LEN] = {0};
int g_replconn_idx = -1;
int g_replication_type = -1;
bool is_cross_region_build = false;
#define RT_WITH_DUMMY_STANDBY 0
#define RT_WITH_MULTI_STANDBY 1

static void walkdir(const char *path, int (*action) (const char *fname, bool isdir), bool process_symlinks);

static void check_repl_uuid(char *repl_uuid)
{
#define IsAlNum(c) (((c) >= 'A' && (c) <= 'Z') || ((c) >= 'a' && (c) <= 'z') || ((c) >= '0' && (c) <= '9'))

    if (repl_uuid == NULL) {
        return;
    }

    int ptr = 0;

    if (strlen(repl_uuid) >= NAMEDATALEN) {
        pg_log(PG_PRINT, _("Max repl_uuid string length is 63.\n"));
        exit(1);
    }

    while (repl_uuid[ptr] != '\0') {
        if (!IsAlNum(repl_uuid[ptr])) {
            pg_log(PG_PRINT, _("repl_uuid only accepts alphabetic or digital character, case insensitive.\n"));
            exit(1);
        }

        repl_uuid[ptr] = tolower(repl_uuid[ptr]);
        ptr++;
    }

    return;
}

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
 * @@GaussDB@@
 * Brief            : BuildCheckReplChannel
 * Description        :
 * Notes            :
 */
static bool BuildCheckReplChannel(const char* ChannelInfo)
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
            if (BuildCheckReplChannel(token)) {
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
bool ParseReplConnInfo(const char* ConnInfoList, int* InfoLength, ReplConnInfo* repl)
{
    int repl_length = 0;
    char* iter = NULL;
    char* pNext = NULL;
    char* ReplStr = NULL;
    char* token = NULL;
    char* ptr = NULL;
    int parsed = 0;
    int iplen = 0;
    char tmp_localhost[IP_LEN] = {0};
    char cascadeToken[IP_LEN] = {0};
    char crossRegionToken[IP_LEN] = {0};
    int tmp_localport = 0;
    char tmp_remotehost[IP_LEN] = {0};
    int tmp_remoteport = 0;
    int tmp_remotenodeid = 0;
    char tmp_remoteuwalhost[IP_LEN] = {0};
    int tmp_remoteuwalport = 0;
    int cascadeLen = strlen("iscascade");
    int corssRegionLen = strlen("isCrossRegion");
    errno_t rc = EOK;
    char* p = NULL;

    if (ConnInfoList == NULL) {
        return false;
    } else {
        ReplStr = strdup(ConnInfoList);
        if (ReplStr == NULL) {
            return false;
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
            return false;
        }

        repl_length = GetLengthAndCheckReplConn(ReplStr);
        if (repl_length == 0) {
            free(ReplStr);
            ReplStr = NULL;
            return false;
        }

        rc = memset_s(repl, sizeof(ReplConnInfo), 0, sizeof(ReplConnInfo));
        securec_check_c(rc, "", "");

        token = strtok_r(ReplStr, ",", &p);
        while (token != NULL) {
            rc = memset_s(tmp_localhost, IP_LEN, 0, sizeof(tmp_localhost));
            securec_check_c(rc, "\0", "\0");

            rc = memset_s(tmp_remotehost, IP_LEN, 0, sizeof(tmp_remotehost));
            securec_check_c(rc, "\0", "\0");

            rc = memset_s(tmp_remoteuwalhost, IP_LEN, 0, sizeof(tmp_remoteuwalhost));
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

            /* remotenodeid */
            iter = strstr(token, "remotenodeid");
            if (iter != NULL) {
                iter += strlen("remotenodeid");
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }

                if (isdigit(*iter)) {
                    tmp_remotenodeid = atoi(iter);
                }
            }

            /* remoteuwalhost */
            iter = strstr(token, "remoteuwalhost");
            if (iter != NULL) {
                iter += strlen("remoteuwalhost");
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }
                if (isdigit(*iter) || *iter == ':' || isalpha(*iter)) {
                    pNext = iter;
                    iplen = 0;
                    while (*pNext != ' ' && 0 != strncmp(pNext, "remoteuwalhost", strlen("remoteuwalhost"))) {
                        iplen++;
                        pNext++;
                    }
                    rc = strncpy_s(tmp_remoteuwalhost, IP_LEN, iter, iplen);
                    securec_check(rc, "", "");

                    tmp_remoteuwalhost[IP_LEN - 1] = '\0';
                }
            }

            /* remoteuwalport */
            iter = strstr(token, "remoteuwalport");
            if (iter != NULL) {
                iter += strlen("remoteuwalport");
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }

                if (isdigit(*iter)) {
                    tmp_remoteuwalport = atoi(iter);
                }
            }

            /* is cascade? */
            iter = strstr(token, "iscascade");
            if (iter != NULL) {
                iter += cascadeLen;
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }
                rc = strncpy_s(cascadeToken, IP_LEN, iter, strlen("true"));
                securec_check_c(rc, "", "");
                if (strcmp(cascadeToken, "true") == 0) {
                    repl->isCascade = true;
                }
            }

            /* is cross region? */
            iter = strstr(token, "iscrossregion");
            if (iter != NULL) {
                iter += corssRegionLen;
                while (*iter == ' ' || *iter == '=') {
                    iter++;
                }
                rc = strncpy_s(crossRegionToken, IP_LEN, iter, strlen("true"));
                securec_check_c(rc, "", "");
                if (strcmp(crossRegionToken, "true") == 0) {
                    repl->isCrossRegion = true;
                }
            }

            /* copy the valus from tmp */
            rc = strncpy_s(repl->localhost, IP_LEN, tmp_localhost, IP_LEN - 1);
            securec_check_c(rc, "", "");

            repl->localhost[IP_LEN - 1] = '\0';
            repl->localport = tmp_localport;

            rc = strncpy_s(repl->remotehost, IP_LEN, tmp_remotehost, IP_LEN - 1);
            securec_check_c(rc, "", "");

            repl->remotehost[IP_LEN - 1] = '\0';
            repl->remoteport = tmp_remoteport;

            rc = strncpy_s(repl->remoteuwalhost, IP_LEN, tmp_remoteuwalhost, IP_LEN - 1);
            securec_check_c(rc, "", "");

            repl->remoteuwalhost[IP_LEN - 1] = '\0';
            repl->remoteuwalport = tmp_remoteuwalport;
            repl->remotenodeid = tmp_remotenodeid;

            token = strtok_r(NULL, ",", &p);
            parsed++;
        }
    }

    free(ReplStr);
    ReplStr = NULL;
    *InfoLength = repl_length;

    return true;
}

/*
 * Brief            : @@GaussDB@@
 * Description        : get available conn
 * Notes            :
 */
void get_conninfo(const char* filename)
{
    char** optlines;
    const char **conninfo_para = NULL;
    int lines_index = 0;
    int optvalue_off;
    int optvalue_len;
    int opt_index = 0;
    errno_t rc = EOK;

    if (filename == NULL) {
        pg_log(PG_PRINT, _("filename is NULL"));
        exit(1);
    }

    if (build_mode == CROSS_CLUSTER_FULL_BUILD || build_mode == CROSS_CLUSTER_INC_BUILD ||
        build_mode == CROSS_CLUSTER_STANDBY_FULL_BUILD || ss_instance_config.dss.enable_dss) {
        /* For shared storage cluster */
        conninfo_para = config_para_cross_cluster_build;
    } else {
        conninfo_para = config_para_build;
    }

    /* cleaning global conninfo list */
    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        rc = memset_s(conninfo_global[i], MAX_VALUE_LEN, 0, MAX_VALUE_LEN);
        securec_check_ss_c(rc, "", "");
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
                    (const char*)conninfo_para[i],
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

        /* read repl_auth_mode */
        lines_index = find_gucoption((const char**)optlines,
            CONFIG_REPL_AUTH_MODE, NULL, NULL, &optvalue_off, &optvalue_len, '\'');
        if (lines_index != INVALID_LINES_IDX) {
            rc = strncpy_s(g_repl_auth_mode, MAX_VALUE_LEN, optlines[lines_index] + optvalue_off,
                (size_t)Min(optvalue_len, MAX_VALUE_LEN - 1));
            securec_check_c(rc, "", "");
        }

        /* read repl_uuid */
        lines_index = find_gucoption((const char**)optlines,
            CONFIG_REPL_UUID, NULL, NULL, &optvalue_off, &optvalue_len, '\'');
        if (lines_index != INVALID_LINES_IDX) {
            rc = strncpy_s(g_repl_uuid, MAX_VALUE_LEN, optlines[lines_index] + optvalue_off,
                (size_t)Min(optvalue_len, MAX_VALUE_LEN - 1));
            securec_check_c(rc, "", "");
        }
        check_repl_uuid(g_repl_uuid);

        pg_log(PG_WARNING, "Get repl_auth_mode is %s and repl_uuid is %s\n", g_repl_auth_mode, g_repl_uuid);

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
    if (conn_get == NULL) {
        pg_log(PG_WARNING, _("build connection failed cause get connection is null.\n"));
        disconnect_and_return_null(conn_get);
    }
    if (PQstatus(conn_get) != CONNECTION_OK) {
        pg_log(PG_WARNING, _("build connection to %s failed cause %s.\n"),
            (conn_get->pghost != NULL) ? conn_get->pghost : conn_get->pghostaddr, PQerrorMessage(conn_get));
        disconnect_and_return_null(conn_get);
    }

    /* 2. IDENTIFY_VERSION */
    if (!check_remote_version(conn_get, term)) {
        disconnect_and_return_null(conn_get);
    }

    /* 3. IDENTIFY_MODE */
    if (!need_copy_upgrade_file) {
        remote_mode = get_remote_mode(conn_get);
        if (remote_mode != NORMAL_MODE && remote_mode != PRIMARY_MODE) {
            disconnect_and_return_null(conn_get);
        }
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
    ReplConnInfo repl_conn_info;
    bool is_uuid_auth = (strcmp(g_repl_auth_mode, REPL_AUTH_MODE_UUID) == 0 && strlen(g_repl_uuid) > 0);
    char uuidOption[MAXPGPATH] = {0};

    if (is_uuid_auth) {
        tnRet = snprintf_s(uuidOption, sizeof(uuidOption), sizeof(uuidOption) - 1, "-c repl_uuid=%s", g_repl_uuid);
        securec_check_ss_c(tnRet, "\0", "\0");
    }

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        bool parseOk = ParseReplConnInfo(conninfo_global[i - 1], &repl_arr_length, &repl_conn_info);
        if (!parseOk) {
            parse_failed_num++;
            continue;
        }
        is_cross_region_build = repl_conn_info.isCrossRegion;

        tnRet = memset_s(repl_conninfo_str, MAXPGPATH, 0, MAXPGPATH);
        securec_check_ss_c(tnRet, "", "");
        is_cross_region_build = false;
        if (register_username != NULL && register_password != NULL) {
            if (*register_username == '.') {
                    register_username += 2;
            }
            tnRet = snprintf_s(repl_conninfo_str,
                sizeof(repl_conninfo_str),
                sizeof(repl_conninfo_str) - 1,
                "localhost=%s localport=%d host=%s port=%d "
                "dbname=postgres replication=hadr_main_standby "
                "fallback_application_name=gs_ctl "
                "connect_timeout=%d rw_timeout=%d "
                "options='-c remotetype=application' user=%s password=%s",
                repl_conn_info.localhost,
                repl_conn_info.localport,
                repl_conn_info.remotehost,
                repl_conn_info.remoteport,
                conn_timeout,
                recv_timeout, register_username, register_password);
            is_cross_region_build = true;
        } else {
            tnRet = snprintf_s(repl_conninfo_str,
                sizeof(repl_conninfo_str),
                sizeof(repl_conninfo_str) - 1,
                "localhost=%s localport=%d host=%s port=%d "
                "dbname=replication replication=true "
                "fallback_application_name=gs_ctl "
                "connect_timeout=%d rw_timeout=%d "
                "options='-c remotetype=application %s'",
                repl_conn_info.localhost,
                repl_conn_info.localport,
                repl_conn_info.remotehost,
                repl_conn_info.remoteport,
                conn_timeout,
                recv_timeout,
                uuidOption);
        }
        securec_check_ss_c(tnRet, "", "");
        con_get = check_and_get_primary_conn(repl_conninfo_str, term);
        tnRet = memset_s(repl_conninfo_str, MAXPGPATH, 0, MAXPGPATH);
        securec_check_ss_c(tnRet, "", "");
        if (is_cross_region_build) {
            if (con_get != NULL && (g_replconn_idx == -1 || i == g_replconn_idx)) {
                g_replconn_idx = i;
                pg_log(PG_WARNING, "build try host(%s) port(%d) success\n", repl_conn_info.remotehost,
                    repl_conn_info.remoteport);
                break;
            }
        } else {
            if (con_get != NULL) {
                pg_log(PG_WARNING, "build try host(%s) port(%d) success\n", repl_conn_info.remotehost,
                    repl_conn_info.remoteport);
                break;
            }
        }
        pg_log(PG_WARNING, "build try host(%s) port(%d) failed\n", repl_conn_info.remotehost,
            repl_conn_info.remoteport);
        if (con_get != NULL) {
            PQfinish(con_get);
            con_get = NULL;
        }
    }

    if (parse_failed_num == MAX_REPLNODE_NUM - 1) {
        pg_log(PG_WARNING, " invalid value for parameter \"replconninfo\" in postgresql.conf.\n");
        if (con_get != NULL)
            PQfinish(con_get);

        return NULL;
    }

    return con_get;
}

/* check connection for standby build standby */
PGconn* check_and_conn_for_standby(int conn_timeout, int recv_timeout, uint32 term)
{
    PGconn* con_get = NULL;
    char repl_conninfo_str[MAXPGPATH];
    ServerMode remote_mode = UNKNOWN_MODE;
    int tnRet = 0;
    int repl_arr_length;
    int i = 0;
    int parse_failed_num = 0;
    ReplConnInfo repl_conn_info;
    bool is_uuid_auth = (strcmp(g_repl_auth_mode, REPL_AUTH_MODE_UUID) == 0 && strlen(g_repl_uuid) > 0);
    char uuidOption[MAXPGPATH] = {0};

    if (is_uuid_auth) {
        tnRet = snprintf_s(uuidOption, sizeof(uuidOption), sizeof(uuidOption) - 1, "-c repl_uuid=%s", g_repl_uuid);
        securec_check_ss_c(tnRet, "\0", "\0");
    }

    for (i = 1; i < MAX_REPLNODE_NUM; i++) {
        bool parseOk = ParseReplConnInfo(conninfo_global[i - 1], &repl_arr_length, &repl_conn_info);
        if (!parseOk || repl_conn_info.isCrossRegion) {
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
            "options='-c remotetype=application %s'",
            repl_conn_info.localhost,
            repl_conn_info.localport,
            repl_conn_info.remotehost,
            repl_conn_info.remoteport,
            conn_timeout,
            recv_timeout,
            uuidOption);
        securec_check_ss_c(tnRet, "", "");


        con_get = PQconnectdb(repl_conninfo_str);
        if (con_get != NULL && PQstatus(con_get) == CONNECTION_OK && check_remote_version(con_get, term)) {
            remote_mode = get_remote_mode(con_get);
            if ((remote_mode == STANDBY_MODE || remote_mode == MAIN_STANDBY_MODE) &&
                (g_replconn_idx == -1 || i == g_replconn_idx)) {
                g_replconn_idx = i;
                pg_log(PG_WARNING, "standby build try host(%s) port(%d) success\n", repl_conn_info.remotehost,
                    repl_conn_info.remoteport);
                break;
            } else {
                PQfinish(con_get);
                con_get = NULL;
            }
        } else {
            if (con_get != NULL) {
                PQfinish(con_get);
                con_get = NULL;
            }
            if (conn_str != NULL) {
                pg_log(PG_WARNING, "The given address can not been access.\n");
                exit(1);
            }
        }
        pg_log(PG_WARNING, "standby build try host(%s) port(%d) failed\n", repl_conn_info.remotehost,
            repl_conn_info.remoteport);
    }

    if (parse_failed_num == MAX_REPLNODE_NUM - 1) {
        pg_log(PG_WARNING, "Invalid value for parameter \"replconninfo\" in postgresql.conf or no correct standby.\n");
        if (con_get != NULL) {
            PQfinish(con_get);
        }
        exit(1);
    }

    return con_get;
}

/*
 * Get paxos value of key from postgres.conf.
 * Value is a char array whose length is MAXPGPATH.
 */
static bool GetDCFKeyValue(const char *filename, const char *key, char *value)
{
    char **optlines;
    int optvalue_off;
    int optvalue_len;
    int line_index = 0;
    int opt_index = 0;

    if (filename == nullptr || key == nullptr || value == nullptr) {
        return false;
    }

    if ((optlines = readfile(filename)) == nullptr) {
        return false;
    }

    line_index = find_gucoption((const char**)optlines,
        key, nullptr, nullptr, &optvalue_off, &optvalue_len);
    if (INVALID_LINES_IDX == line_index) {
        return false;
    }
    int len = strlen(optlines[line_index] + optvalue_off);
    const int minLen = 2; /* There is at least a '\n' and a character in the value. */
    if (len < minLen) {
        return false;
    }
    errno_t rc = strcpy_s(value, MAXPGPATH, optlines[line_index] + optvalue_off);
    securec_check_c(rc, "\0", "\0");
    value[len - 1] = '\0'; /* Remove '\n'. */
    pg_log(PG_WARNING, _("DCF path is %s\n"), value);
    while (optlines[opt_index] != nullptr) {
        free(optlines[opt_index]);
        optlines[opt_index] = nullptr;
        opt_index++;
    }

    free(optlines);
    optlines = nullptr;
    /* Remove single quote from value */
    if (value != nullptr && value[0] == '\'') {
        int i = 0;
        int len = strlen(value);
        while (i < len) {
            value[i] = value[i + 1];
            i++;
        }
        const int singleToEnd = 2;
        int endSingleQIdx = len - singleToEnd;
        if (endSingleQIdx >= 0 && value[endSingleQIdx] == '\'') {
            value[endSingleQIdx] = '\0';
        }
    }
    pg_log(PG_WARNING, _("Final DCF path is %s\n"), value);
    return true;
}

int IsBeginWith(const char *str1, char *str2)
{
    if (str1 == NULL || str2 == NULL) {
        return -1;
    }
    int len1 = strlen(str1);
    int len2 = strlen(str2);
    if ((len1 < len2) || (len1 == 0 || len2 == 0)) {
        return -1;
    }

    char *p = str2;
    int i = 0;
    while (*p != '\0') {
        if (*p != str1[i]) {
            return 0;
        }
        p++;
        i++;
    }
    return 1;
}

bool SsIsSkipPath(const char* dirname, bool needskipall)
{   
    if (!ss_instance_config.dss.enable_dss) {
        return false;    
    }

    if (strcmp(dirname, ".recycle") == 0) {
        return true;    
    }

    /* skip doublewrite of all instances*/
    if (IsBeginWith(dirname, "pg_doublewrite") > 0) {
        return true;
    }

    if (IsBeginWith(dirname, "pg_replication") > 0) {
        return true;
    }

    /* skip pg_control file when dss enable, only copy pg_control of main standby,
     * we need to retain pg_control of other nodes, so pg_contol not be deleted directly.
     */
    if (strcmp(dirname, "pg_control") == 0) {
        return true;
    }

    /* skip directory which not belong to primary in dss */
    if (needskipall) {
        /* skip pg_xlog and doublewrite of all instances*/
        if (IsBeginWith(dirname, "pg_xlog") > 0) {
            return true;
        }
    } else {
        /* skip other node pg_xlog except primary */
        if (IsBeginWith(dirname, "pg_xlog") > 0) { 
            size_t dirNameLen = strlen("pg_xlog");
            char instanceId[MAX_INSTANCEID_LEN] = {0};
            errno_t rc = EOK;
            rc = snprintf_s(instanceId, sizeof(instanceId), sizeof(instanceId) - 1, "%d",
                                ss_instance_config.dss.instance_id);
            securec_check_ss_c(rc, "\0", "\0");
            /* not skip pg_xlog directory in file systerm */
            if (strlen(dirname) > dirNameLen && strcmp(dirname + dirNameLen, instanceId) != 0)
                return true;
        }
    }
    return false;
}

static void DeleteSubDataDir(const char* dirname)
{
    DIR* dir = NULL;
    char fullpath[MAXPGPATH] = {0};
    struct dirent* de = NULL;
    struct stat st;
    errno_t rc = 0;
    int nRet = 0;
    /* data dir */
    /* Delete dcf data path first for it maybe not under data dir */
    char pgConfFile[MAXPGPATH] = {0};
    nRet = snprintf_s(pgConfFile, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", dirname);
    securec_check_ss_c(nRet, "", "");
    bool enableDCF = GetPaxosValue(pgConfFile);
    char dcfLogPath[MAXPGPATH] = {0};
    bool hasLogPath = false;
    if (enableDCF) {
        char dcfDataPath[MAXPGPATH] = {0};
        bool hasDataPath = GetDCFKeyValue(pgConfFile, "dcf_data_path", dcfDataPath);
        if (hasDataPath) {
            /* Don't remove the dcf data path if it didn't exist. */
            if (lstat(dcfDataPath, &st) != 0) {
                pg_log(PG_WARNING, _("could not stat file or directory %s.\n"), dcfDataPath);
            } else {
                if (!rmtree(dcfDataPath, true)) {
                    pg_log(PG_WARNING, _("failed to remove dcf data dir %s.\n"), dcfDataPath);
                    exit(1);
                }
                pg_log(PG_WARNING, _("Remove dcf data dir %s.\n"), dcfDataPath);
            }
        }
        hasLogPath = GetDCFKeyValue(pgConfFile, "dcf_log_path", dcfLogPath);
    }

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
            if (g_is_obsmode && (strcmp(de->d_name, "pg_replslot") == 0))
                continue;
            if (is_dss_file(dirname) && SsIsSkipPath(de->d_name, true))
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
            /* Don't delete dcf log path */
            if (enableDCF && hasLogPath) {
                char comparedFullPath[MAXPGPATH] = {0};
                char comparedLogPath[MAXPGPATH] = {0};
                nRet = snprintf_s(comparedFullPath, MAXPGPATH, MAXPGPATH - 1, "%s/", fullpath);
                securec_check_ss_c(nRet, "", "");
                nRet = snprintf_s(comparedLogPath, MAXPGPATH, MAXPGPATH - 1, "%s/", dcfLogPath);
                securec_check_ss_c(nRet, "", "");
                int comparedFullPathLen = strlen(comparedFullPath);
                if (strncmp(comparedFullPath, comparedLogPath, comparedFullPathLen) == 0) {
                    hasLogPath = false;
                    pg_log(PG_WARNING, _("Skip dcf log path %s.\n"), dcfLogPath);
                    continue;
                }
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
                                /* enable dss, something in pg_replslot may be a link */
                                if (unlink(fullpath) != 0) {
                                    pg_log(PG_WARNING, _("failed to remove dir %s,errno=%d.\n"), fullpath, errno);
                                    (void)closedir(dir);
                                    exit(1);
                                }
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
                    strcmp(de->d_name, "postgresql.conf.bak") == 0 || strcmp(de->d_name, "postgresql.conf.guc.bak") == 0 ||
                    strcmp(de->d_name, "build_completed.start") == 0 || strcmp(de->d_name, "gs_build.pid") == 0 ||
                    strcmp(de->d_name, "postmaster.opts") == 0 || strcmp(de->d_name, "gaussdb.state") == 0 ||
                    strcmp(de->d_name, "disc_readonly_test") == 0 || strcmp(de->d_name, ssl_cert_file) == 0 ||
                    strcmp(de->d_name, ssl_key_file) == 0 || strcmp(de->d_name, ssl_ca_file) == 0 ||
                    strcmp(de->d_name, ssl_crl_file) == 0 || strcmp(de->d_name, ssl_cipher_file) == 0 ||
                    strcmp(de->d_name, ssl_rand_file) == 0 || 
#ifdef USE_TASSL
                    strcmp(de->d_name, ssl_enc_cert_file) == 0 || strcmp(de->d_name, ssl_enc_key_file) == 0 ||
                    strcmp(de->d_name, ssl_enc_cipher_file) == 0 || strcmp(de->d_name, ssl_enc_rand_file) == 0 ||
#endif 
                    strcmp(de->d_name, "rewind_lable") == 0 || strcmp(de->d_name, "gs_gazelle.conf") == 0 ||
                    (g_is_obsmode && strcmp(de->d_name, "base.tar.gz") == 0) ||
                    (g_is_obsmode && strcmp(de->d_name, "pg_hba.conf") == 0)||
                    (g_is_obsmode && strcmp(de->d_name, "pg_ident.conf") == 0) ||
                    (IS_CROSS_CLUSTER_BUILD && strcmp(de->d_name, "pg_hba.conf") == 0) ||
                    strcmp(de->d_name, "pg_hba.conf.old") == 0)
                    continue;
                
                /* Skip paxos index files for building process will write them */
                if (enableDCF && ((strcmp(de->d_name, "paxosindex") == 0) ||
                    (strcmp(de->d_name, "paxosindex.backup") == 0)))
                    continue;
                /* build from cn reserve this file,om will modify it. */
                if ((conn_str != NULL) && strncmp(de->d_name, "pg_hba.conf", strlen("pg_hba.conf")) == 0) {
                    continue;
                }
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
    if (strncmp(dirname, "+", 1) == 0 ) {
        nRet = snprintf_s(xlogpath, MAXPGPATH, sizeof(xlogpath) - 1, "%s/pg_xlog%d", dirname,
        ss_instance_config.dss.instance_id);
    } else {
        nRet = snprintf_s(xlogpath, MAXPGPATH, sizeof(xlogpath) - 1, "%s/pg_xlog", dirname);
    }
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

    DeleteSubDataDir(dirname);
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
        for (i = 1; i < DOUBLE_MAX_REPLNODE_NUM; i++) {
            const char *para = NULL;
            if (i > MAX_REPLNODE_NUM) {
                para = config_para_cross_cluster_build[i - MAX_REPLNODE_NUM];
            } else if (i < MAX_REPLNODE_NUM) {
                para = config_para_build[i];
            }
            lines_index = find_gucoption(
                (const char**)optlines, (const char*)para, NULL, NULL, &optvalue_off, &optvalue_len);

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
            ReplConnInfo repl_conn_info;
            int repl_arr_length = 0;
            bool parseOk = ParseReplConnInfo(conninfo_global[g_replconn_idx], &repl_arr_length, &repl_conn_info);
            if (parseOk) {
                errorno = snprintf_s(slotname, len, len - 1, "%s_%s_%d", pgxcnodename, repl_conn_info.localhost, 
                    repl_conn_info.localport);
                securec_check_ss_c(errorno, "", "");
            }
        }
    }

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
        "select * from pg_catalog.pg_cbm_rotate_file('%08X/%08X'); ",
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

/* Get whether paxos is enable from postgres.conf */
bool GetPaxosValue(const char *filename)
{
    char **optlines;
    int optvalue_off;
    int optvalue_len;
    const char *paxosPara = "enable_dcf";
    int line_index = 0;
    bool ret = false;
    int opt_index = 0;

    if (filename == nullptr) {
        return false;
    }

    if ((optlines = readfile(filename)) != nullptr) {
        line_index = find_gucoption((const char**)optlines, 
                                    paxosPara, nullptr, nullptr, &optvalue_off, &optvalue_len);
        if (INVALID_LINES_IDX != line_index) {
            if (strcmp("true\n", optlines[line_index] + optvalue_off) == 0 ||
                strcmp("on\n", optlines[line_index] + optvalue_off) == 0 ||
                strcmp("\'on\'\n", optlines[line_index] + optvalue_off) == 0)
                ret = true;
        }
        while (optlines[opt_index] != nullptr) {
            free(optlines[opt_index]);
            optlines[opt_index] = nullptr;
            opt_index++;
        }

        free(optlines);
        optlines = nullptr;
        return ret;
    } else {
        return false;
    }
}

/*
 * Issue fsync recursively on PGDATA and all its contents.
 *
 * We fsync regular files and directories wherever they are, but we follow
 * symlinks only for pg_wal (or pg_xlog) and immediately under pg_tblspc.
 * Other symlinks are presumed to point at files we're not responsible for
 * fsyncing, and might not have privileges to write at all.
 *
 */
void fsync_pgdata(const char *pg_data)
{
    bool xlog_is_symlink = false;
    char pg_xlog[MAXPGPATH] = {0};
    char pg_tblspc[MAXPGPATH] = {0};
    errno_t errorno = EOK;

    if (is_dss_file(pg_data)) {
        errorno = snprintf_s(pg_xlog, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d", pg_data,
                             ss_instance_config.dss.instance_id);
    } else {
        errorno = snprintf_s(pg_xlog, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog", pg_data);
    }
    securec_check_ss_c(errorno, "\0", "\0");
    errorno = snprintf_s(pg_tblspc, MAXPGPATH, MAXPGPATH - 1, "%s/pg_tblspc", pg_data);
    securec_check_ss_c(errorno, "\0", "\0");

#ifndef WIN32
    {
        struct stat st;

        if (lstat(pg_xlog, &st) < 0) {
            pg_log(PG_WARNING, _("could not stat file \"%s\": %s\n"), pg_xlog, strerror(errno));
            exit(1);
        }
        else if (S_ISLNK(st.st_mode))
            xlog_is_symlink = true;
    }
#else
    if (pgwin32_is_junction(pg_xlog))
        xlog_is_symlink = true;
#endif

    /*
     * Now we do the fsync()s in the same order.
     *
     * The main call ignores symlinks, so in addition to specially processing
     * pg_wal if it's a symlink, pg_tblspc has to be visited separately with
     * process_symlinks = true.  Note that if there are any plain directories
     * in pg_tblspc, they'll get fsync'd twice.  That's not an expected case
     * so we don't worry about optimizing it.
     */
    walkdir(pg_data, fsync_fname, false);
    if (xlog_is_symlink)
        walkdir(pg_xlog, fsync_fname, false);
    walkdir(pg_tblspc, fsync_fname, true);
}

/*
 * walkdir: recursively walk a directory, applying the action to each
 * regular file and directory (including the named directory itself).
 *
 * If process_symlinks is true, the action and recursion are also applied
 * to regular files and directories that are pointed to by symlinks in the
 * given directory; otherwise symlinks are ignored.  Symlinks are always
 * ignored in subdirectories, ie we intentionally don't pass down the
 * process_symlinks flag to recursive calls.
 *
 * Errors are reported but not considered fatal.
 *
 * See also walkdir in fd.cpp, which is a backend version of this logic.
 */
static void walkdir(const char *path, int (*action) (const char *fname, bool isdir), bool process_symlinks)
{
    DIR *dir;
    struct dirent *de = NULL;
    errno_t errorno = EOK;

    dir = opendir(path);
    if (dir == NULL) {
        pg_log(PG_WARNING, _("could not open directory \"%s\": %s\n"), path, strerror(errno));
        return;
    }

    while (errno = 0, (de = readdir(dir)) != NULL) {
        char subpath[MAXPGPATH * 2] = {0};
        struct stat fst;
        int sret;

        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
            continue;

        if (strcmp(de->d_name, "pg_ctl.lock") == 0) {
            continue;
        }
        errorno = snprintf_s(subpath, sizeof(subpath), sizeof(subpath) - 1, "%s/%s", path, de->d_name);
        securec_check_ss_c(errorno, "\0", "\0");

        if (process_symlinks)
            sret = stat(subpath, &fst);
        else
            sret = lstat(subpath, &fst);
        if (sret < 0) {
            pg_log(PG_WARNING, _("could not stat file \"%s\": %s\n"), subpath, strerror(errno));
            continue;
        }

        if (S_ISREG(fst.st_mode))
            (*action) (subpath, false);
        else if (S_ISDIR(fst.st_mode))
            walkdir(subpath, action, false);
    }

    if (errno)
        pg_log(PG_WARNING, _("could not read directory \"%s\": %s\n"), path, strerror(errno));

    (void)closedir(dir);

    /*
     * It's important to fsync the destination directory itself as individual
     * file fsyncs don't guarantee that the directory entry for the file is
     * synced.  Recent versions of ext4 have made the window much wider but
     * it's been an issue for ext3 and other filesystems in the past.
     */
    (*action) (path, true);
}

/*
 * fsync_fname -- Try to fsync a file or directory
 *
 * Ignores errors trying to open unreadable files, or trying to fsync
 * directories on systems where that isn't allowed/required.  All other errors
 * are fatal.
 */
int fsync_fname(const char *fname, bool isdir)
{
    int fd = -1;
    int flags;
    int returncode;

    /*
     * Some OSs require directories to be opened read-only whereas other
     * systems don't allow us to fsync files opened read-only; so we need both
     * cases here.  Using O_RDWR will cause us to fail to fsync files that are
     * not writable by our userid, but we assume that's OK.
     */
    flags = PG_BINARY;
    if (!isdir)
        flags |= O_RDWR;
    else
        flags |= O_RDONLY;

    /*
     * Open the file, silently ignoring errors about unreadable files (or
     * unsupported operations, e.g. opening a directory under Windows), and
     * logging others.
     */
    fd = open(fname, flags, 0);
    if (fd < 0) {
        if (errno == EACCES || (isdir && (errno == EISDIR || errno == ERR_DSS_FILE_TYPE_MISMATCH)))
            return 0;
        pg_log(PG_WARNING, _("could not open file \"%s\": %s\n"), fname, strerror(errno));
        return -1;
    }

    returncode = fsync(fd);

    /*
     * Some OSes don't allow us to fsync directories at all, so we can ignore
     * those errors. Anything else needs to be reported.
     */
    if (returncode != 0 && !(isdir && (errno == EBADF || errno == EINVAL))) {
        pg_log(PG_WARNING, _("could not fsync file \"%s\": %s\n"), fname, strerror(errno));
        (void) close(fd);
        exit(EXIT_FAILURE);
    }

    (void) close(fd);
    return 0;
}
