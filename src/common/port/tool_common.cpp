/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * tool_common.cpp
 *
 * IDENTIFICATION
 *        src/common/port/tool_common.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <stdio.h>
#include "storage/file/fio_device.h"
#include "securec.h"
#include "securec_check.h"
#include "bin/elog.h"
#include "tool_common.h"

SSInstanceConfig ss_instance_config = {
    .dss = {
        .enable_dss = false,
        .enable_dorado = false,
        .enable_stream = false,
        .instance_id = 0,
        .primaryInstId = -1,
        .interNodeNum = 0,
        .vgname = NULL,
        .vglog = NULL,
        .vgdata = NULL,
        .socketpath = NULL,
    },
};

datadir_t g_datadir;  /* need init when used in first time */

static void initFileDataPathStruct(datadir_t *dataDir);
static void initDSSDataPathStruct(datadir_t *dataDir);
static int ss_get_inter_node_nums(const char* interconn_url);

void initDataPathStruct(bool enable_dss)
{
    if (enable_dss) {
        initDSSDataPathStruct(&g_datadir);
    } else {
        initFileDataPathStruct(&g_datadir);
    }
}

static void initFileDataPathStruct(datadir_t *dataDir)
{
    errno_t rc = EOK;

    // dir path
    rc = snprintf_s(dataDir->baseDir, MAXPGPATH, MAXPGPATH - 1, "%s/base", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->globalDir, MAXPGPATH, MAXPGPATH - 1, "%s/global", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->locationDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_location", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->tblspcDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_tblspc", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->clogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_clog", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->csnlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_csnlog", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->notifyDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_notify", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->serialDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_serial", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->snapshotsDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_snapshots", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->twophaseDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_twophase", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->multixactDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_multixact", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->xlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    // sub-file path
    rc = snprintf_s(dataDir->controlPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_control", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlBakPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_control.backup", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwOldPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwPathPrefix, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw_", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwSinglePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw_single", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBuildPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw.build", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwMetaPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/pg_dw_meta", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwUpgradePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/dw_upgrade", dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeMetaPath, MAXPGPATH, MAXPGPATH - 1, "%s/global/dw_batch_upgrade_meta",
        dataDir->pg_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeFilePath, MAXPGPATH, MAXPGPATH - 1, "%s/global/dw_batch_upgrade_files",
        dataDir->pg_data);
    securec_check_ss_c(rc, "", "");
}

static void initDSSDataPathStruct(datadir_t *dataDir)
{
    errno_t rc = EOK;

    // DSS file directory (cluster owner)
    rc = snprintf_s(dataDir->baseDir, MAXPGPATH, MAXPGPATH - 1, "%s/base", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->globalDir, MAXPGPATH, MAXPGPATH - 1, "%s/global", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->locationDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_location", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->tblspcDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_tblspc", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlBakPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_control.backup", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->controlInfoPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_replication/pg_ss_ctl_info", dataDir->dss_data);
    securec_check_ss_c(rc, "", "");

    // DSS file directory (instance owner)
    rc = snprintf_s(dataDir->clogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_clog%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->csnlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_csnlog%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->serialDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_serial%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->snapshotsDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_snapshots%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->twophaseDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_twophase%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->multixactDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_multixact%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->xlogDir, MAXPGPATH, MAXPGPATH - 1, "%s/pg_xlog%d", dataDir->dss_data,
        dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    // Unix file directory (instance owner)
    rc = snprintf_s(dataDir->dwDir.dwOldPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwPathPrefix, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw_",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwSinglePath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw_single",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBuildPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw.build",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwUpgradePath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/dw_upgrade",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwMetaPath, MAXPGPATH, MAXPGPATH - 1, "%s/pg_doublewrite%d/pg_dw_meta",
        dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeMetaPath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/dw_batch_upgrade_meta", dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");

    rc = snprintf_s(dataDir->dwDir.dwBatchUpgradeFilePath, MAXPGPATH, MAXPGPATH - 1,
        "%s/pg_doublewrite%d/dw_batch_upgrade_files", dataDir->dss_data, dataDir->instance_id);
    securec_check_ss_c(rc, "", "");
}

char *getSocketpathFromEnv()
{
    char* env_value = NULL;
    env_value = getenv("DSS_HOME");
    if ((env_value == NULL) || (env_value[0] == '\0')) {
        return NULL;
    }

    char *file = (char*)malloc(MAXPGPATH);
    errno_t rc = EOK;
    rc = snprintf_s(file, MAXPGPATH, MAXPGPATH - 1, "UDS:%s/.dss_unix_d_socket", env_value);
    securec_check_ss_c(rc, "\0", "\0");

    return file;
}

/*
 * Brief            : @@GaussDB@@
 * Description    : find the value of guc para according to name
 * Notes            :
 */
int find_gucoption(
    const char** optlines, const char* opt_name, int* name_offset, int* name_len,
    int* value_offset, int* value_len, unsigned char strip_char)
{
#define SKIP_CHAR(c, skip_c) (isspace((c)) || (c) == (skip_c))

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
        while (SKIP_CHAR(((unsigned char)*p), strip_char)) {
            p++;
        }
        q = p;
        while (*q && !(*q == '\n' || *q == '#')) {
            if (!(SKIP_CHAR(((unsigned char)*q), strip_char))) {
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
 * @@GaussDB@@
 * Brief			:
 * Description		:
 * Notes			:
 */
int find_guc_optval(const char** optlines, const char* optname, char* optval)
{
    int offset = 0;
    int len = 0;
    int lineno = 0;
    char def_optname[64];
    int ret;
    errno_t rc = EOK;

    lineno = find_gucoption(optlines, (const char*)optname, NULL, NULL, &offset, &len, '\'');
    if (lineno != INVALID_LINES_IDX) {
        rc = strncpy_s(optval, MAX_VALUE_LEN, optlines[lineno] + offset, (size_t)(Min(len, MAX_VALUE_LEN)));
        securec_check_c(rc, "", "");
        return lineno;
    }

    ret = snprintf_s(def_optname, sizeof(def_optname), sizeof(def_optname) - 1, "#%s", optname, '\'');
    securec_check_ss_c(ret, "\0", "\0");
    lineno = find_gucoption(optlines, (const char*)def_optname, NULL, NULL, &offset, &len);
    if (lineno != INVALID_LINES_IDX) {
        rc = strncpy_s(optval, MAX_VALUE_LEN, optlines[lineno] + offset, (size_t)(Min(len, MAX_VALUE_LEN)));
        securec_check_c(rc, "", "");
        return lineno;
    }
    return INVALID_LINES_IDX;
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
        result = (char**)palloc(sizeof(char*));
        if (result == NULL) {
            return NULL;
        }
        *result = NULL;
        return result;
    }
    if (statbuf.st_size + 1 > MAX_CONFIG_FILE_SIZE) {
        /* empty file */
        close(fd);
        result = (char**)palloc(sizeof(char*));
        if (result == NULL) {
            return NULL;
        }
        *result = NULL;
        return result;
    }

    buffer = (char*)palloc(statbuf.st_size + 1);
    if (buffer == NULL) {
        close(fd);
        return NULL;
    }

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
    result = (char**)palloc((nlines + 1) * sizeof(char*));
    if (result == NULL) {
        free(buffer);
        buffer = NULL;
        return NULL;
    }

    /* now split the buffer into lines */
    linebegin = buffer;
    n = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            int slen = &buffer[i] - linebegin + 1;
            char* linebuf = (char*)palloc(slen + 1);
            if (linebuf == NULL) {
                free(buffer);
                buffer = NULL;
                return result;
            }
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

void freefile(char** lines)
{
    char** line = NULL;
    if (lines == NULL)
        return;
    line = lines;
    while (*line != NULL) {
        free(*line);
        *line = NULL;
        line++;
    }
    free(lines);
}

/*
* read ss config, return enable_dss 
* we will get ss_enable_dss, ss_dss_conn_path and ss_dss_vg_name.
*/
bool ss_read_config(const char* pg_data)
{
    char config_file[MAXPGPATH] = {0};
    char enable_dss[MAXPGPATH] = {0};
    char enable_dorado[MAXPGPATH] = {0};
    char enable_stream[MAXPGPATH] = {0};
    char inst_id[MAXPGPATH] = {0};
    char interconnect_url[MAXPGPATH] = {0};
    char** optlines = NULL;
    int ret = EOK;

    ret = snprintf_s(config_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", pg_data);
    securec_check_ss_c(ret, "\0", "\0");
    config_file[MAXPGPATH - 1] = '\0';
    optlines = readfile(config_file);

    (void)find_guc_optval((const char**)optlines, "ss_enable_dss", enable_dss);

    /* this is not enable_dss, wo do not need to do anythiny else */
    if(strcmp(enable_dss, "on") != 0) {
        freefile(optlines);
        optlines = NULL;
        return false;
    }

    (void)find_guc_optval((const char**)optlines, "ss_enable_dorado", enable_dorado);
    if (strcmp(enable_dorado, "on") == 0) {
        ss_instance_config.dss.enable_dorado = true;
    }

    (void)find_guc_optval((const char**)optlines, "ss_stream_cluster", enable_stream);
    if (strcmp(enable_stream, "on") == 0) {
        ss_instance_config.dss.enable_stream = true;
    }

    ss_instance_config.dss.enable_dss = true;
    ss_instance_config.dss.socketpath = (char*)malloc(sizeof(char) * MAXPGPATH);
    ss_instance_config.dss.vgname = (char*)malloc(sizeof(char) * MAXPGPATH);
    (void)find_guc_optval((const char**)optlines, "ss_dss_conn_path", ss_instance_config.dss.socketpath);
    (void)find_guc_optval((const char**)optlines, "ss_dss_vg_name", ss_instance_config.dss.vgname);
    (void)find_guc_optval((const char**)optlines, "ss_instance_id", inst_id);
    ss_instance_config.dss.instance_id = atoi(inst_id);

    (void)find_guc_optval((const char**)optlines, "ss_interconnect_url", interconnect_url);
    ss_instance_config.dss.interNodeNum = ss_get_inter_node_nums(interconnect_url);

    freefile(optlines);
    optlines = NULL;
    return true;
}

int SSInitXlogDir(char*** xlogDirs)
{
    int xlogDirNum = 0;
    *xlogDirs = (char**)malloc(SS_MAX_INST * sizeof(char*));
    if (*xlogDirs == NULL) {
        return -1;
    }

    for (int i = 0; i < SS_MAX_INST; i++) {
        (*xlogDirs)[i] = (char*)malloc(MAXPGPATH * sizeof(char));
        if ((*xlogDirs)[i] == NULL) {
            for (int j = 0; j < i; j++) {
                free((*xlogDirs)[j]);
            }
            free(*xlogDirs);
            return -1;
        }
    }

    DIR* dir = opendir(ss_instance_config.dss.vgname);
    struct dirent* entry = NULL;
    while (dir != NULL && (entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "pg_xlog", strlen("pg_xlog")) == 0) {
            int rc = snprintf_s((*xlogDirs)[xlogDirNum], MAXPGPATH, MAXPGPATH - 1, "%s/%s",
                ss_instance_config.dss.vgname, entry->d_name);
            securec_check_ss_c(rc, "", "");
            xlogDirNum++;
            if (xlogDirNum >= SS_MAX_INST) {
                break;
            }
        }
    }
    closedir(dir);
    return xlogDirNum;
}

void FreeXlogDir(char** xlogDirs)
{
    if (ss_instance_config.dss.enable_dss && xlogDirs != NULL) {
        for (int i = 0; i < SS_MAX_INST; i++) {
            free(xlogDirs[i]);
        }
        free(xlogDirs);
    }
}

static int ss_get_inter_node_nums(const char* interconn_url)
{
    errno_t rc;
    int nodeNum = 0;
    char* next_token = NULL;
    char* token = NULL;
    const char* delim = ",";
    if (interconn_url == NULL || interconn_url[0] == '\0') {
        fprintf(stdout, "can not contain interconnect nodes.\n");
        return nodeNum;
    }

    char* strs = (char*)palloc(strlen(interconn_url) + 1);
    if (strs == NULL) {
        return nodeNum;
    }
    rc = strncpy_s(strs, strlen(interconn_url) + 1, interconn_url, strlen(interconn_url));
    securec_check_c(rc, "\0", "\0");

    token = strtok_s(strs, delim, &next_token);
    do {
        nodeNum++;
        token = strtok_s(NULL, delim, &next_token);
    } while (token != NULL);
    pfree(strs);
    return nodeNum;
}
