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
 * nas_am.h
 *    nas access method definitions.
 * 
 * IDENTIFICATION
 *    src/gausskernel/storage/access/archive/nas_am.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <assert.h>
#include <vector>
#include <string>
#include <iostream>
#include <stdio.h>
#include <libgen.h>

#include "access/obs/nas_am.h"

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/value.h"
#include "pgstat.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "storage/lock/lwlock.h"
#include "securec.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/plog.h"
#include "postmaster/alarmchecker.h"
#include "replication/walreceiver.h"

#define MAX_PATH_LEN 1024

size_t NasRead(const char* fileName, const int offset, char *buffer, const int length, ObsArchiveConfig *nas_config)
{
    size_t readLength = 0;
    char file_path[MAXPGPATH] = {0};
    int ret = 0;
    FILE *fp = NULL;
    struct stat statbuf;

    if ((fileName == NULL) || (buffer == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("The parameter cannot be NULL")));
    }

    if (nas_config == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot get archive config from replication slots")));
    }

    ret = snprintf_s(file_path, MAXPGPATH, MAXPGPATH - 1, "%s%s", nas_config->obs_prefix, fileName);
    securec_check_ss(ret, "\0", "\0");

    if (stat(file_path, &statbuf)) {
        if (errno != ENOENT) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not stat file \"%s\": %m", fileName)));
        }
        ereport(ERROR, (errcode_for_file_access(), errmsg("The file \"%s\" not exists", fileName)));
        return readLength;
    }

    canonicalize_path(file_path);
    fp = fopen(file_path, "rb");
    if (fp == NULL) {
        ereport(ERROR, (errcode_for_file_access(), errmsg("could not read file \"%s\": %m", fileName)));
        return readLength;
    }
    if (statbuf.st_size > length) {
        fclose(fp);
        ereport(ERROR, (errcode_for_file_access(), errmsg("file size is wrong, \"%s\": %m", fileName)));
        return readLength;
    }

    readLength = fread(buffer, 1, statbuf.st_size, fp);

    fclose(fp);
    return readLength;
}

int NasWrite(const char* fileName, const char *buffer, const int bufferLength, ObsArchiveConfig *nas_config)
{
    int ret = 0;
    ObsArchiveConfig *archive_nas = nas_config;
    char file_path[MAXPGPATH] = {0};
    char *origin_file_path = NULL;
    char *base_path = NULL;
    FILE *fp = NULL;

    if ((fileName == NULL) || (buffer == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("The parameter cannot be NULL")));
    }

    if (archive_nas == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot get archive config from replication slots")));
    }

    ret = snprintf_s(file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s/%s", archive_nas->obs_prefix, XLOGDIR, fileName);
    securec_check_ss(ret, "\0", "\0");

    canonicalize_path(file_path);

    origin_file_path = pstrdup(file_path);
    base_path = dirname(origin_file_path);
    if (!isDirExist(base_path)) {
        if (pg_mkdir_p(base_path, S_IRWXU) != 0) {
            pfree_ext(origin_file_path);
            ereport(LOG, (errmsg("could not create path \"%s\"", base_path)));
            return -1;
        }
    }

    fp = fopen(file_path, "wb");
    if (fp == NULL) {
        pfree_ext(origin_file_path);
        ereport(LOG, (errmsg("could not create file \"%s\": %m", fileName)));
        return -1;
    }

    if (fwrite(buffer, bufferLength, 1, fp) != 1) {
        pfree_ext(origin_file_path);
        fclose(fp);
        return -1;
    }

    pfree_ext(origin_file_path);
    fclose(fp);
    return 0;
}

int NasDelete(const char* fileName, ObsArchiveConfig *nas_config)
{
    int ret = 0;
    struct stat statbuf;
    char file_path[MAXPGPATH] = {0};

    if (nas_config == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot get obs bucket config from replication slots")));
    }

    ret = snprintf_s(file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", nas_config->obs_prefix, fileName);
    securec_check_ss(ret, "\0", "\0");

    if (lstat(file_path, &statbuf) < 0) {
        return -1;
    }
    if (S_ISDIR(statbuf.st_mode)) {
        return (rmdir(file_path));
    }
    return (unlink(file_path));
}

/*
 * Obtain files with specified prefix in archive directory, unsorted
 * The prefix can be:
 * 1. path
 * 2. filename
 * 3. the prefix of filename
 */
static List* GetNasFileList(const char* prefix, ObsArchiveConfig *nas_config)
{
    int ret = 0;
    char file_path[MAXPGPATH] = {0};
    char path_buf[MAXPGPATH] = {0};
    char *origin_file_path = NULL;
    char *base_path = NULL;
    List* fileNameList = NIL;
    struct dirent* de = NULL;
    struct stat st;
    bool isDir = false;

    if (prefix == NULL || nas_config == NULL || nas_config->obs_prefix == NULL) {
        ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("The parameter cannot be NULL")));
    }

    ret = snprintf_s(file_path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", nas_config->obs_prefix, prefix);
    securec_check_ss(ret, "\0", "\0");

    canonicalize_path(file_path);

    if (stat(file_path, &st) == 0 && S_ISDIR(st.st_mode)) { // is dir,
        isDir = true;
        base_path = file_path;
    } else {    // may be the file_path is filename or the prefix of filename
        struct stat st_base;
        origin_file_path = pstrdup(file_path);
        base_path = dirname(origin_file_path);
        if (stat(base_path, &st_base) != 0) {
            ereport(LOG, (errmsg("WARNING: there is no file in dir %s", file_path)));
            return NIL;    // the base_path not exists, return NIL
        }
    }

    DIR *dir = opendir(base_path);
    while ((de = readdir(dir)) != NULL) {
        if (isDir) {
            if ((strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)) {
                continue;
            }
            ret = snprintf_s(path_buf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", base_path, de->d_name);
            securec_check_ss_c(ret, "\0", "\0");
            if (lstat(path_buf, &st) != 0) {
                continue;
            }
            /* only find file */
            if (S_ISREG(st.st_mode)) {
                fileNameList = lappend(fileNameList, pstrdup(path_buf));
            } else {
                continue;
            }
        } else {
            if (strncmp(de->d_name, basename(file_path), strlen(basename(file_path))) == 0) {
                ret = snprintf_s(path_buf, MAXPGPATH, MAXPGPATH - 1, "%s/%s", base_path, de->d_name);
                securec_check_ss_c(ret, "\0", "\0");
                fileNameList = lappend(fileNameList, pstrdup(path_buf));
            } else {
                continue;
            }
        }
    }

    closedir(dir);
    pfree_ext(origin_file_path);
    return fileNameList;
}

static int CompareFileNames(const void* a, const void* b)
{
    char* fna = *((char**)a);
    char* fnb = *((char**)b);

    return strcmp(fna, fnb);
}


static List* SortFileList(List* file_list)
{
    int file_num;
    char** files;
    ListCell* lc = NULL;
    List* result = NIL;
    int i = 0;

    file_num = list_length(file_list);
    if (file_num < 1) {
        return NIL;
    }

    files = (char**)palloc0(file_num * sizeof(char*));
    foreach (lc, file_list) {
        files[i++] = (char*)lfirst(lc);
    }
    qsort(files, file_num, sizeof(char*), CompareFileNames);
    for (i = 0; i < file_num; i++) {
        result = lappend(result, pstrdup(files[i]));
    }

    pfree_ext(files);
    return result;
}

List* NasList(const char* prefix, ObsArchiveConfig *nas_config)
{
    List* fileNameList = NIL;
    List* fileNameListTmp = NIL;

    if (nas_config == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Cannot get archive config from replication slots")));
    }

    fileNameListTmp = GetNasFileList(prefix, nas_config);

    fileNameList = SortFileList(fileNameListTmp);

    list_free_ext(fileNameListTmp);
    return fileNameList;
}
