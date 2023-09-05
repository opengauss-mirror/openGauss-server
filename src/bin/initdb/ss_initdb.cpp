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
                                       "+pg_replslot"};

static const char* ss_instancedirs[] = {"+pg_xlog",
                                        "+pg_doublewrite"
                                        };

static const char* ss_instanceowndirs[] = {"base",
                                           "base/1",
                                           "global",
                                           "pg_xlog",
                                           "pg_xlog/archive_status",
                                           "undo",
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

static const char* ss_xlogsubdirs[] = {"archive_status"};

/* for ss dorado replication */
static const char* ss_specialdirs[] = {"+pg_replication"};

/* num of every directory type */
#define SS_CLUSTERDIRS_NUM       ARRAY_NUM(ss_clusterdirs)
#define SS_INSTANCEDIRS_NUM      ARRAY_NUM(ss_instancedirs)
#define SS_INSTANCEOENDIRS_NUM   ARRAY_NUM(ss_instanceowndirs)
#define SS_XLOGSUBIRS_NUM        ARRAY_NUM(ss_xlogsubdirs)
#define SS_SPECIALDIRS_NUM       ARRAY_NUM(ss_specialdirs)

char *ss_nodedatainfo = NULL;
int32 ss_nodeid = INVALID_INSTANCEID;
bool ss_issharedstorage = false;
bool ss_need_mkclusterdir = true;
bool ss_need_mkspecialdir = false;
static const char *ss_progname = "ss_initdb";

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

static char *ss_concat_path(int32 node_id, const char *parent_dir, const char *dir)
{
    char *path = NULL;
    char *prepath = NULL;
    char nodeid_str[MAXPGPATH];
    size_t len = strlen(parent_dir) + 2 + strlen(dir);
    
    /* prepared path by connecting vgname and subdir */
    prepath = (char *)pg_malloc(len);
    errno_t sret = sprintf_s(prepath, len, "%s/%s", parent_dir, dir + 1);
    securec_check_ss_c(sret, prepath, "\0");
    
    if (node_id != INVALID_INSTANCEID) {
        pg_ltoa(node_id, nodeid_str);
        len = len + strlen(nodeid_str);
        path = (char *)pg_malloc(len);
    
        /* full path by connecting prepared path and node id */
        sret = sprintf_s(path, len, "%s%d", prepath, node_id);
        securec_check_ss_c(sret, path, "\0");
    } else {
        path = (char *)pg_malloc(len);
        sret = sprintf_s(path, len, "%s", prepath);
        securec_check_ss_c(sret, path, "\0");
    }
    
    FREE_AND_RESET(prepath);
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

int ss_check_existdir(const char *path, int node_id, const char **subdir)
{
    char *subpath = NULL;
    struct stat statbuf;
    int existnum = 0;
    int totalnum = ARRAY_NUM(subdir);

    for (uint32 i = 0; i < totalnum; i++) {
        subpath = ss_concat_path(node_id, path, subdir[i]);
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

bool ss_check_exist_specialdir(char *path)
{
    for (uint32_t i = 0; i < SS_SPECIALDIRS_NUM; ++i) {
        if (strcmp(ss_specialdirs[i] + 1, path) == 0) {
            return true;
        }
    }
    return false;
}

bool ss_check_specialdir(char *path)
{
    char *datadir = path;
    DIR *pgdatadir = NULL;
    struct dirent *file = NULL;
    
    if ((pgdatadir = opendir(datadir)) != NULL) {
        while ((file = readdir(pgdatadir)) != NULL) {
            if (strcmp(".", file->d_name) == 0 || strcmp("..", file->d_name) == 0) {
                /* skip this and parent directory */
                continue;
            }

            if (ss_check_exist_specialdir(file->d_name)) {
                (void)closedir(pgdatadir);
                return true;
            }
        }
        (void)closedir(pgdatadir);
    }

    return false;
}

int ss_check_shareddir(char *path, int node_id, bool *need_mkclusterdir)
{
    int ret = 0;
    *need_mkclusterdir = false;

    // step1: check cluster dir, must exists when instance id is not 0
    switch (ss_check_existdir(path, INVALID_INSTANCEID, ss_clusterdirs)) {
        case 0:
            *need_mkclusterdir = true;
            break;
        case 1:
            ret |= ERROR_CLUSTERDIR_INCOMPLETE; // cluster dir exists but not complete
            break;
        case 2:
            *need_mkclusterdir = false;
            break;
        default:
            fprintf(stderr, _("unkown state for ss_clusterdirs.\n"));
    }

    // node 0 must be primary in initdb
    if (node_id != 0 && (*need_mkclusterdir)) {
        ret |= ERROR_CLUSTERDIR_NO_EXISTS_BY_STANDBY;
    }

    if (node_id == 0 && !(*need_mkclusterdir)) {
        ret |= ERROR_CLUSTERDIR_EXISTS_BY_PRIMARY;
    }

    // step2: check instancedir dir, do not allow exists
    switch (ss_check_existdir(path, node_id, ss_instancedirs)) {
        case 0:
            break;
        case 1:
        case 2:
            ret |= ERROR_INSTANCEDIR_EXISTS;
            fprintf(stderr, _("instancedir already exists.\n"));
            break;
        default:
            fprintf(stderr, _("unkown state for ss_instancedirs.\n"));
    }

    return ret;
}

void ss_mkdirdir(int32 node_id, const char *pg_data, const char *vgdata_dir, const char *vglog_dir,
    bool need_mkclusterdir, bool need_specialdir)
{
    /* Create required subdirectories */
    printf(_("creating subdirectories ... in shared storage mode ... "));
    (void)fflush(stdout);

    /* unshared and instance one copy */
    ss_createdir(ss_instanceowndirs, SS_INSTANCEOENDIRS_NUM, INVALID_INSTANCEID, pg_data, vgdata_dir, vglog_dir);

    /* shared and instance one copy */
    ss_createdir(ss_instancedirs, SS_INSTANCEDIRS_NUM, node_id, pg_data, vgdata_dir, vglog_dir);

    /* shared and cluster one copy */
    if (need_mkclusterdir) {
        ss_createdir(ss_clusterdirs, SS_CLUSTERDIRS_NUM, INVALID_INSTANCEID, pg_data, vgdata_dir, vglog_dir);
    }

    if (need_specialdir) {
        ss_createdir(ss_specialdirs, SS_SPECIALDIRS_NUM, INVALID_INSTANCEID, pg_data, vgdata_dir, vglog_dir);
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

void ss_createdir(const char **ss_dirs, int32 num, int32 node_id, const char *pg_data, const char *vgdata_dir,
    const char *vglog_dir)
{
    int i;
    for (i = 0; i < num; i++) {
        char *path = NULL;
        errno_t sret = 0;
        size_t len = 0;
        bool is_dss = is_dss_file(ss_dirs[i]);
        bool is_xlog = false;
        char *link_path = NULL;

        if (vglog_dir[0] != '\0' && (pg_strcasecmp(ss_dirs[i], "+pg_xlog") == 0 ||
            pg_strcasecmp(ss_dirs[i], "+pg_notify") == 0 ||
            pg_strcasecmp(ss_dirs[i], "+pg_snapshots") == 0 ||
            pg_strcasecmp(ss_dirs[i], "+pg_replication") == 0)) {
            is_xlog = true;
        }

        if (is_dss) {
            if (is_xlog) {
                path = ss_concat_path(node_id, vglog_dir, ss_dirs[i]);
                link_path = ss_concat_path(node_id, vgdata_dir, ss_dirs[i]);
            } else {
                path = ss_concat_path(node_id, vgdata_dir, ss_dirs[i]);
            }
        } else {
            len = strlen(pg_data) + strlen(ss_dirs[i]) + 1 + 1;
            path = (char *)pg_malloc(len);
            sret = sprintf_s(path, len, "%s/%s", pg_data, ss_dirs[i]);
            securec_check_ss_c(sret, path, "\0");
        }

        ss_makedirectory(path);
        if (is_xlog) {
            symlink(path, link_path);
        }

        /* create suddirectory of pg_xlog/pg_multixact/pg_llog */
        if (is_dss && pg_strcasecmp(ss_dirs[i] + 1, "pg_xlog") == 0) {
            ss_makesubdir(path, ss_xlogsubdirs, SS_XLOGSUBIRS_NUM);
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
