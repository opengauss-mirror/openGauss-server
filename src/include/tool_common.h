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
 * tool_common.h
 * 
 * IDENTIFICATION
 *        src/include/tool_common.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef TOOL_COMMON_H
#define TOOL_COMMON_H

#include "storage/file/fio_device_com.h"

#define MAXPGPATH 1024
#define SS_MAX_INST 64
#define MAX_PARAM_LEN 1024
#define INVALID_LINES_IDX (int)(~0)
#define MAX_VALUE_LEN 1024
#define MAX_CONFIG_FILE_SIZE 0xFFFFF /* max file size for configurations = 1M */

#ifndef palloc
#define palloc(sz) malloc(sz)
#endif
#ifndef pfree
#define pfree(ptr) free(ptr)
#endif

#define T_SS_XLOGDIR \
    (g_enable_dss ? g_datadir.xlogDir : "pg_xlog")
#define T_DEFTBSDIR \
    (g_enable_dss ? g_datadir.baseDir : "base")
#define T_GLOTBSDIR \
    (g_enable_dss ? g_datadir.globalDir : "global")
#define T_CLOGDIR \
    (g_enable_dss ? g_datadir.clogDir : "pg_clog")
#define T_CSNLOGDIR \
    (g_enable_dss ? g_datadir.csnlogDir : "pg_csnlog")
#define T_PG_LOCATION_DIR \
    (g_enable_dss ? g_datadir.locationDir : "pg_location")
#define T_NOTIFYDIR \
    (g_enable_dss ? g_datadir.notifyDir : "pg_notify")
#define T_SERIALDIR \
    (g_enable_dss ? g_datadir.serialDir : "pg_serial")
#define T_SNAPSHOT_EXPORT_DIR \
    (g_enable_dss ? g_datadir.snapshotsDir : "pg_snapshots")
#define T_TBLSPCDIR \
    (g_enable_dss ? g_datadir.tblspcDir : "pg_tblspc")
#define T_TWOPHASE_DIR \
    (g_enable_dss ? g_datadir.twophaseDir : "pg_twophase")
#define T_MULTIXACTDIR \
    (g_enable_dss ? g_datadir.multixactDir : "pg_multixact")
#define T_XLOG_CONTROL_FILE \
    (g_enable_dss ? g_datadir.controlPath : "global/pg_control")
#define T_XLOG_CONTROL_FILE_BAK \
    (g_enable_dss ? g_datadir.controlBakPath : "global/pg_control.backup")
#define T_OLD_DW_FILE_NAME \
    (g_enable_dss ? g_datadir.dwDir.dwOldPath : "global/pg_dw")
#define T_DW_FILE_NAME_PREFIX \
    (g_enable_dss ? g_datadir.dwDir.dwPathPrefix : "global/pg_dw_")
#define T_SINGLE_DW_FILE_NAME \
    (g_enable_dss ? g_datadir.dwDir.dwSinglePath : "global/pg_dw_single")
#define T_DW_BUILD_FILE_NAME \
    (g_enable_dss ? g_datadir.dwDir.dwBuildPath : "global/pg_dw.build")
#define T_DW_UPGRADE_FILE_NAME \
    (g_enable_dss ? g_datadir.dwDir.dwUpgradePath : "global/dw_upgrade")
#define T_DW_BATCH_UPGRADE_META_FILE_NAME \
    (g_enable_dss ? g_datadir.dwDir.dwBatchUpgradeMetaPath : "global/dw_batch_upgrade_meta")
#define T_DW_BATCH_UPGRADE_BATCH_FILE_NAME \
    (g_enable_dss ? g_datadir.dwDir.dwBatchUpgradeFilePath : "global/dw_batch_upgrade_files")
#define T_DW_META_FILE \
    (g_enable_dss ? g_datadir.dwDir.dwMetaPath : "global/pg_dw_meta")

typedef struct st_dw_subdatadir_t {
    char dwOldPath[MAXPGPATH];
    char dwPathPrefix[MAXPGPATH];
    char dwSinglePath[MAXPGPATH];
    char dwBuildPath[MAXPGPATH];
    char dwUpgradePath[MAXPGPATH];
    char dwBatchUpgradeMetaPath[MAXPGPATH];
    char dwBatchUpgradeFilePath[MAXPGPATH];
    char dwMetaPath[MAXPGPATH];
} dw_subdatadir_t;

typedef struct st_datadir_t {
    char pg_data[MAXPGPATH];    // pg_data path in unix
    char dss_data[MAXPGPATH];   // dss vgdata (only in dss mode)
    char dss_log[MAXPGPATH];    // dss vglog (only in dss mode)
    int instance_id;            // instance id of cluster (only in dss mode)
    char xlogDir[MAXPGPATH];
    char baseDir[MAXPGPATH];
    char globalDir[MAXPGPATH];
    char clogDir[MAXPGPATH];
    char csnlogDir[MAXPGPATH];
    char locationDir[MAXPGPATH];
    char notifyDir[MAXPGPATH];
    char serialDir[MAXPGPATH];
    char snapshotsDir[MAXPGPATH];
    char tblspcDir[MAXPGPATH];
    char twophaseDir[MAXPGPATH];
    char multixactDir[MAXPGPATH];
    char controlPath[MAXPGPATH];
    char controlBakPath[MAXPGPATH];
    char controlInfoPath[MAXPGPATH];
    dw_subdatadir_t dwDir;
} datadir_t;

/* DSS conntct parameters */
typedef struct DssOptions {
    bool enable_dss;
    bool enable_dorado;
    bool enable_stream;
    int instance_id;
    int primaryInstId;
    int interNodeNum;
    char *vgname;
    char *vglog;
    char *vgdata;
    char *socketpath;
} DssOptions;

typedef struct SSInstanceConfig {
    DssOptions dss;
} SSInstanceConfig;

extern SSInstanceConfig ss_instance_config;
extern datadir_t g_datadir;

void initDataPathStruct(bool enable_dss);

char *getSocketpathFromEnv();

int find_gucoption(
    const char** optlines, const char* opt_name, int* name_offset, int* name_len,
    int* value_offset, int* value_len, unsigned char strip_char = ' ');
int find_guc_optval(const char** optlines, const char* optname, char* optval);

char** readfile(const char* path);
void freefile(char** lines);

bool ss_read_config(const char* pg_data);
int SSInitXlogDir(char*** xlogDirs);
void FreeXlogDir(char** xlogDirs);

#endif
