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
 * ---------------------------------------------------------------------------------------
 *
 * ss_initdb.cpp
 *
 *
 * IDENTIFICATION
 *        src/bin/initdb/ss_initdb.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <cipher.h>

#include "access/ustore/undo/knl_uundoapi.h"
#include "access/ustore/undo/knl_uundotxn.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "getaddrinfo.h"
#include "getopt_long.h"
#include "miscadmin.h"
#include "bin/elog.h"
#include "ss_initdb.h"
#include "storage/file/fio_device.h"


static const char* ss_clusterdirs[] = {"+global",
                                       "+base",
                                       "+base/1",
                                       "+pg_tblspc",
                                       "+pg_clog",
                                       "+pg_csnlog",
                                       "+pg_multixact",
                                       "+pg_multixact/members",
                                       "+pg_multixact/offsets",
                                       "+pg_twophase",
                                       "+pg_serial",
                                       "+pg_replslot",
                                       "+pg_xlog",
                                       "+pg_xlog/archive_status",
                                       "+pg_doublewrite",
                                       "+pg_replication"};    // for ss dorado replication

static const char* ss_instanceowndirs[] = {"base",
                                           "base/1",
                                           "global",
                                           "pg_xlog",
                                           "pg_xlog/archive_status",
                                           "pg_stat_tmp",
                                           "pg_errorinfo",
                                           "pg_logical",
                                           "pg_llog",
                                           "pg_llog/snapshots",
                                           "pg_llog/mappings",
                                           "pg_clog",
                                           "pg_notify",
                                           "pg_csnlog",
                                           "pg_multixact",
                                           "pg_multixact/members",
                                           "pg_multixact/offsets",
                                           "pg_snapshots"};

/* num of every directory type */
#define SS_CLUSTERDIRS_NUM       ARRAY_NUM(ss_clusterdirs)
#define SS_INSTANCEOENDIRS_NUM   ARRAY_NUM(ss_instanceowndirs)

char *ss_nodedatainfo = NULL;
int32 ss_nodeid = INVALID_INSTANCEID;
bool ss_issharedstorage = false;
bool need_create_data = true;
static const char *ss_progname = "ss_initdb";

#define FREE_AND_RESET(ptr)  \
    do {                     \
        if (NULL != (ptr) && reinterpret_cast<char*>(ptr) != static_cast<char*>("")) { \
            free(ptr);       \
            (ptr) = NULL;    \
        }                    \
    } while (0)

void ss_createdir(const char **ss_dirs, int32 num, const char *pg_data, const char *vgdata_dir, const char *vglog_dir);

/*
 * pg_ltoa: converts a signed 32-bit integer to its string representation
 *
 * Caller must ensure that 'a' points to enough memory to hold the result
 * (at least 12 bytes, counting a leading sign and trailing NUL).
 */
void pg_ltoa(int32 value, char *a)
{
    char *start = a;
    bool neg = false;
    errno_t ss_rc;

    if (a == NULL) {
        return;
    }

    /*
     * Avoid problems with the most negative integer not being representable
     * as a positive integer.
     */
    if (value == (-2147483647 - 1)) {
        const int a_len = 12;
        ss_rc = memcpy_s(a, a_len, "-2147483648", a_len);
        securec_check(ss_rc, "\0", "\0");
        return;
    } else if (value < 0) {
        value = -value;
        neg = true;
    }

    /* Compute the result string backwards. */
    do {
        int32 remainder;
        int32 oldval = value;

        value /= 10;
        remainder = oldval - value * 10;
        *a++ = (char)('0' + remainder);
    } while (value != 0);

    if (neg) {
        *a++ = '-';
    }

    /* Add trailing NUL byte, and back up 'a' to the last character. */
    *a-- = '\0';

    /* Reverse string. */
    while (start < a) {
        char swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
}

static char *ss_concat_path(const char *parent_dir, const char *dir)
{
    char *path = NULL;
    errno_t sret = EOK;
    size_t len = strlen(parent_dir) + 2 + strlen(dir);

    /* prepared path by connecting vgname and subdir */
    path = (char *)pg_malloc(len);
    if (is_dss_file(dir)) {
        sret = sprintf_s(path, len - 1, "%s/%s", parent_dir, dir + 1);
    } else {
        sret = sprintf_s(path, len, "%s/%s", parent_dir, dir);
    }
    securec_check_ss_c(sret, path, "\0");
    return path;
}

/* check dms url when gs_initdb */
bool ss_check_nodedatainfo(bool enable_dss)
{   
    bool issharedstorage = false;

    if (!enable_dss) {
        if (ss_nodeid != INVALID_INSTANCEID || ss_nodedatainfo != NULL) {
            fprintf(stderr, _("ss_nodeid is valid or nodedatainfo exist without enable-dss.\n"));
            exit(1);
        }
        return issharedstorage;   
    }

    if ((ss_nodeid == INVALID_INSTANCEID || ss_nodedatainfo == NULL)) {
        fprintf(stderr, _("ss_nodeid is invalid or nodedatainfo not exist or nodedatainfo is empty with enable-dss.\n"));
        exit(1);
    }

    if (ss_nodeid != INVALID_INSTANCEID && ss_nodedatainfo != NULL) {
        issharedstorage = true;
    }

    return issharedstorage;
}

int ss_check_existdir(const char *path, const char **subdir)
{
    char *subpath = NULL;
    struct stat statbuf;
    int existnum = 0;
    int totalnum = (int)ARRAY_NUM(subdir);

    for (int i = 0; i < totalnum; i++) {
        subpath = ss_concat_path(path, subdir[i]);
        if (lstat(subpath, &statbuf) == 0) {
            existnum++;
        }
        FREE_AND_RESET(subpath);
    }

    if (existnum == 0) {
        return 0; // subdir do not exists
    } else if (existnum < totalnum) {
        return 1; // subdir exists but not complete
    }
    return 2; // subdir exists and complete
}

int ss_check_shareddir(char *path, int node_id, bool *need_mkclusterdir)
{
    int ret = 0;
    *need_mkclusterdir = false;

    // check cluster dir, must exists when instance id is not 0
    switch (ss_check_existdir(path, ss_clusterdirs)) {
        case 0:
            *need_mkclusterdir = true;
            break;
        case 1:
            ret = ERROR_CLUSTERDIR_INCOMPLETE; // cluster dir exists but not complete
            break;
        case 2:
            *need_mkclusterdir = false;
            break;
        default:
            fprintf(stderr, _("unkown state for ss_clusterdirs.\n"));
    }

    // node 0 must be primary in initdb
    if (ret == 0) {
        if (node_id != 0 && (*need_mkclusterdir)) {
            ret = ERROR_CLUSTERDIR_NO_EXISTS_BY_STANDBY;
        } else if (node_id == 0 && !(*need_mkclusterdir)) {
            ret = ERROR_CLUSTERDIR_EXISTS_BY_PRIMARY;
        }
    }

    return ret;
}

void ss_mkdirdir(const char *pg_data, const char *vgdata_dir, const char *vglog_dir, bool need_mkclusterdir)
{
    /* Create required subdirectories */
    printf(_("creating subdirectories ... in shared storage mode ... "));
    (void)fflush(stdout);

    /* unshared and instance one copy */
    ss_createdir(ss_instanceowndirs, SS_INSTANCEOENDIRS_NUM, pg_data, vgdata_dir, vglog_dir);

    /* shared and cluster one copy */
    if (need_mkclusterdir) {
        ss_createdir(ss_clusterdirs, SS_CLUSTERDIRS_NUM, pg_data, vgdata_dir, vglog_dir);
    }
}

void ss_makedirectory(char *path)
{
    /*
     * The parent directory already exists, so we only need mkdir() not
     * pg_mkdir_p() here, which avoids some failure modes; cf bug #13853.
     */
    if (mkdir(path, S_IRWXU) < 0) {
        char errBuffer[ERROR_LIMIT_LEN];
        fprintf(stderr, _("%s: could not create directory \"%s\": %s\n"), ss_progname, path,
            pqStrerror(errno, errBuffer, ERROR_LIMIT_LEN));
        (void)fflush(stdout);
        exit_nicely();
    }
}

void ss_makesubdir(char *path, const char **subdir, uint num)
{
    size_t len = 0;
    errno_t sret = 0;

    for (int i = 0; (unsigned int)i < num; i++) {
        len = strlen(path) + strlen(subdir[i]) + 1 + 1;
        char *subpath = NULL;
        subpath = (char *)pg_malloc(len);
        sret = sprintf_s(subpath, len, "%s/%s", path, subdir[i]);
        securec_check_ss_c(sret, subpath, "\0");
        ss_makedirectory(subpath);
        FREE_AND_RESET(subpath);
    }
}

void ss_createdir(const char **ss_dirs, int32 num, const char *pg_data, const char *vgdata_dir, const char *vglog_dir)
{
    int i;
    for (i = 0; i < num; i++) {
        char *path = NULL;
        bool is_dss = is_dss_file(ss_dirs[i]);
        bool is_xlog = false;
        char *link_path = NULL;

        if (vglog_dir[0] != '\0' && (pg_strcasecmp(ss_dirs[i], "+pg_xlog") == 0 ||
            pg_strcasecmp(ss_dirs[i], "+pg_replication") == 0)) {
            is_xlog = true;
        }

        if (is_dss) {
            if (is_xlog) {
                path = ss_concat_path(vglog_dir, ss_dirs[i]);
                link_path = ss_concat_path(vgdata_dir, ss_dirs[i]);
            } else {
                path = ss_concat_path(vgdata_dir, ss_dirs[i]);
            }
        } else {
            path = ss_concat_path(pg_data, ss_dirs[i]);
        }
        ss_makedirectory(path);

        if (is_xlog) {
            symlink(path, link_path);
        }

        FREE_AND_RESET(path);
        FREE_AND_RESET(link_path);
    }
}

/* ss_addnodeparmater
 * function: add the extra parameter for share storage for dms during gs_Initdb
 * input: conflines    char**   parameter of postgresql.conf had been added previuosly
 * output: conflines   char**   parameter of postgresql.conf to be added in this function
 */
char **ss_addnodeparmater(char **conflines)
{
    if (!ss_issharedstorage) {
        return conflines;
    }

    int nRet = 0;
    char repltok[TZ_STRLEN_MAX + 100];

    fputs(_("adding dms parameters to configuration files ... "), stdout);
    (void)fflush(stdout);

    nRet = sprintf_s(repltok, sizeof(repltok), "ss_instance_id = %d", ss_nodeid);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#ss_instance_id = 0", repltok);

    nRet = strcpy_s(repltok, sizeof(repltok), "ss_enable_dms = on");
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#ss_enable_dms = off", repltok);

    nRet = sprintf_s(repltok, sizeof(repltok), "ss_interconnect_url = '%s'", ss_nodedatainfo);
    securec_check_ss_c(nRet, "\0", "\0");
    conflines = replace_token(conflines, "#ss_interconnect_url = '0:127.0.0.1:1611'", repltok);

    return conflines;
}
