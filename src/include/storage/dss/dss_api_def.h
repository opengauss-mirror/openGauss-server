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
 * dss_api_def.h
 *        Defines the DSS data structure.
 *
 *
 * IDENTIFICATION
 *        src/include/storage/dss/dss_api_def.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef __DSS_API_DEF_H
#define __DSS_API_DEF_H

#include "time.h"

struct __dss_dir;
typedef struct __dss_dir *dss_dir_handle;

typedef enum en_dss_log_level {
    DSS_LOG_LEVEL_ERROR = 0,  // error conditions
    DSS_LOG_LEVEL_WARN,       // warning conditions
    DSS_LOG_LEVEL_INFO,       // informational messages
    DSS_LOG_LEVEL_COUNT,
} dss_log_level_t;

typedef enum en_dss_log_id {
    DSS_LOG_ID_RUN = 0,
    DSS_LOG_ID_DEBUG,
    DSS_LOG_ID_COUNT,
} dss_log_id_t;

#define DSS_SEEK_MAXWR 3                         /* Used for seek actual file size for openGauss */
#define DSS_MAX_NAME_LEN 64                      /* Consistent with dss_defs.h */
#define DSS_FILE_PATH_MAX_LENGTH (SIZE_K(1) + 1) /* Consistent with dss_defs.h */
/* make the dss handle start from this value, to be distinguished from file system handle value */
#define DSS_HANDLE_BASE 0x20000000
#define DSS_CONN_NEVER_TIMEOUT (-1)
#define DSS_VERSION_MAX_LEN 256

typedef enum en_dss_item_type { DSS_PATH, DSS_FILE, DSS_LINK } dss_item_type_t;

typedef struct st_dss_dirent {
    dss_item_type_t d_type;
    char d_name[DSS_MAX_NAME_LEN];
} dss_dirent_t;

typedef enum en_dss_server_status {
    DSS_STATUS_NORMAL = 0,
    DSS_STATUS_READONLY,
    DSS_STATUS_READWRITE,
} dss_server_status_t;

typedef struct st_dss_stat {
    unsigned long long size;
    unsigned long long written_size;
    time_t create_time;
    time_t update_time;
    char name[DSS_MAX_NAME_LEN];
    dss_item_type_t type;
} dss_stat_t;

typedef struct st_dss_dirent *dss_dir_item_t;
typedef struct st_dss_stat *dss_stat_info_t;

typedef void (*dss_log_output)(dss_log_id_t log_type, dss_log_level_t log_level, const char *code_file_name,
    unsigned int code_line_num, const char *module_name, const char *format, ...);

#define DSS_LOCAL_MAJOR_VER_WEIGHT  1000000
#define DSS_LOCAL_MINOR_VER_WEIGHT  1000
#define DSS_LOCAL_MAJOR_VERSION     0
#define DSS_LOCAL_MINOR_VERSION     0
#define DSS_LOCAL_VERSION           5

#define DSS_SUCCESS 0
#define DSS_ERROR (-1)

/** 1.DSS range [2000, 2500] *
 *  2.ERR_DSS_SUBMODEL_ACTION_DETAIL, _DETAIL is optional which indicates the error cause.
 */
#define ERR_DSS_FLOOR 2000
// vg error [2000, 2050)
#define ERR_DSS_VG_CREATE 2000
#define ERR_DSS_VG_LOCK 2010
#define ERR_DSS_VG_REMOVE 2020
#define ERR_DSS_VG_CHECK 2030
#define ERR_DSS_VG_CHECK_NOT_INIT 2031
#define ERR_DSS_VG_NOT_EXIST 2040

// volumn error [2050, 2130)
#define ERR_DSS_VOLUME_SYSTEM_IO 2050
#define ERR_DSS_VOLUME_OPEN 2060
#define ERR_DSS_VOLUME_READ 2070
#define ERR_DSS_VOLUME_WRITE 2080
#define ERR_DSS_VOLUME_SEEK 2090
#define ERR_DSS_VOLUME_ADD 2100
#define ERR_DSS_VOLUME_ADD_EXISTED 2101
#define ERR_DSS_VOLUME_REMOVE 2110
#define ERR_DSS_VOLUME_REMOVE_NOEXIST 2111
#define ERR_DSS_VOLUME_REMOVE_NONEMPTY 2112
#define ERR_DSS_VOLUME_REMOVE_SUPER_BLOCK 2113

// file error [2130, 2230)
#define ERR_DSS_FILE_SEEK 2130
#define ERR_DSS_FILE_REMOVE 2140
#define ERR_DSS_FILE_REMOVE_OPENING 2141
#define ERR_DSS_FILE_RENAME 2150
#define ERR_DSS_FILE_RENAME_DIFF_VG 2151
#define ERR_DSS_FILE_RENAME_EXIST 2152
#define ERR_DSS_FILE_RENAME_OPENING_REMOTE 2153
#define ERR_DSS_FILE_CLOSE 2160
#define ERR_DSS_FILE_CREATE 2170
#define ERR_DSS_FILE_RDWR 2180
#define ERR_DSS_FILE_RDWR_INSUFF_PER 2181
#define ERR_DSS_FILE_NOT_EXIST 2190
#define ERR_DSS_FILE_OPENING_REMOTE 2191
#define ERR_DSS_FILE_TYPE_MISMATCH 2192
#define ERR_DSS_FILE_PATH_ILL 2193
#define ERR_DSS_FILE_INVALID_SIZE 2194
#define ERR_DSS_FILE_INVALID_WRITTEN_SIZE 2195

// dir error [2230, 2280)
#define ERR_DSS_DIR_REMOVE 2230
#define ERR_DSS_DIR_REMOVE_NOT_EMPTY 2231
#define ERR_DSS_DIR_CREATE 2240
#define ERR_DSS_DIR_CREATE_DUPLICATED 2241
// link error [2280, 2300)
#define ERR_DSS_LINK_READ 2280
#define ERR_DSS_LINK_READ_NOT_LINK 2281
#define ERR_DSS_LINK_CREATE 2290

// config error [2300, 2320)
#define ERR_DSS_CONFIG_FILE_OVERSIZED 2300
#define ERR_DSS_CONFIG_LOAD 2301
#define ERR_DSS_CONFIG_LINE_OVERLONG 2302

// redo error [2320, 2350)
#define ERR_DSS_REDO_ILL 2320

// Basic Data Structure error [2350, 2400)
#define ERR_DSS_OAMAP_INSERT 2350
#define ERR_DSS_OAMAP_INSERT_DUP_KEY 2351
#define ERR_DSS_OAMAP_FETCH 2352
#define ERR_DSS_SKLIST_ERR 2360
#define ERR_DSS_SKLIST_NOT_INIT 2361
#define ERR_DSS_SKLIST_NOT_EXIST 2362
#define ERR_DSS_SKLIST_EXIST 2363
#define ERR_DSS_SHM_CREATE 2370
#define ERR_DSS_SHM_CHECK 2371
#define ERR_DSS_SHM_LOCK 2372
#define ERR_DSS_GA_INIT 2380
#define ERR_DSS_GA_GET_ADDR 2381
#define ERR_DSS_SESSION_INVALID_ID 2390
#define ERR_DSS_SESSION_CREATE 2391

// other error [2400, 2500)
#define ERR_DSS_INVALID_PARAM 2400
#define ERR_DSS_NO_SPACE 2401
#define ERR_DSS_ENV_NOT_INITIALIZED 2402
#define ERR_DSS_CLI_EXEC_FAIL 2403
#define ERR_DSS_FNODE_CHECK 2404
#define ERR_DSS_LOCK_TIMEOUT 2405
#define ERR_DSS_SERVER_IS_DOWN 2406
#define ERR_DSS_CHECK_SIZE 2407
#define ERR_DSS_MES_ILL 2408
#define ERR_DSS_STRING_TOO_LONG 2409
#define ERR_DSS_TCP_TIMEOUT_REMAIN 2410
#define ERR_DSS_UDS_INVALID_URL 2411
#define ERR_DSS_RECV_MSG_FAILED 2412
#define ERR_DSS_INIT_LOGGER_FAILED 2414
#define ERR_DSS_OUT_OF_MEM 2415
#define ERR_DSS_INVALID_ID 2416
#define ERR_DSS_PROCESS_REMOTE 2417
#define ERR_DSS_CONNECT_FAILED 2418
#define ERR_DSS_VERSION_NOT_MATCH 2419
#define ERR_DSS_INVALID_BLOCK_TYPE 2420
#define ERR_DSS_CEIL 2500

#endif  // __DSS_API_DEF_H
