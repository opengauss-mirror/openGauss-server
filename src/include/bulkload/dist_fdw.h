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
 * dist_fdw.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/bulkload/dist_fdw.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __DIST_FDW_H__
#define __DIST_FDW_H__

#include "postgres.h"
#include "knl/knl_variable.h"
#include "fmgr.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "storage/buf/block.h"
#include "storage/smgr/fd.h"
#include "nodes/parsenodes.h"
#include "bulkload/importerror.h"

extern "C" Datum dist_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum dist_fdw_validator(PG_FUNCTION_ARGS);

extern "C" Datum file_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum file_fdw_validator(PG_FUNCTION_ARGS);

extern "C" Datum hdfs_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum hdfs_fdw_validator(PG_FUNCTION_ARGS);

#ifdef ENABLE_MOT
extern "C" Datum mot_fdw_validator(PG_FUNCTION_ARGS);
extern "C" Datum mot_fdw_handler(PG_FUNCTION_ARGS);
#endif

extern void encryptOBSForeignTableOption(List** options);

/*
 * in shared mode coordinator assign file to data node
 */
typedef struct DistFdwFileSegment {
    NodeTag type;
    char* filename;
    long begin;
    long end; /* -1 means read the end of file*/
    int64 ObjectSize;
} DistFdwFileSegment;

typedef struct DistFdwDataNodeTask {
    NodeTag type;
    char* dnName;
    List* task;
} DistFdwDataNodeTask;

typedef struct {
    NodeTag type;
    int logger_num;              /* number of loggers */
    RangeTblEntry* rte;          /* error relation*/
    char* filename;              /* path of cache file*/
    ImportErrorLogger** loggers; /* error logger */
} ErrorCacheEntry;

extern const char* optLocation;
extern const char* optFormat;
extern const char* optHeader;
extern const char* optDelimiter;
extern const char* optQutote;
extern const char* optEscape;
extern const char* optNull;
extern const char* optEncoding;
extern const char* optFillMissFields;
extern const char* optRejectLimit;
extern const char* optMode;
extern const char* optForceNotNull;
extern const char* optWriteOnly;
extern const char* optWithoutEscaping;
extern const char* optErrorRel;
extern const char* optFormatter;
extern const char* optFix;
extern const char* optFileHeader;
extern const char* optOutputFilePrefix;
extern const char* optOutputFixAlignment;

extern const char* optLogRemote;
extern const char* optSessionKey;
extern const char* optTaskList;
extern const char* optIgnoreExtraData;

/* OBS specific options */
extern const char* optChunkSize;
extern const char* optAsync;
extern const char* optEncrypt;
extern const char* optAccessKey;
extern const char* optSecretAccessKey;
#define DEST_CIPHER_LENGTH 1024

/*
 * bulkload compatible illegal chars option
 */
extern const char* optCompatibleIllegalChars;
/*
 * bulkload datetime format options
 */
extern const char* optDateFormat;
extern const char* optTimeFormat;
extern const char* optTimestampFormat;
extern const char* optSmalldatetimeFormat;

extern const char* GSFS_PREFIX;
extern const char* GSFSS_PREFIX;
extern const char* LOCAL_PREFIX;
extern const char* ROACH_PREFIX;
extern const char* GSOBS_PREFIX;
extern const int GSFS_PREFIX_LEN;
extern const int GSFSS_PREFIX_LEN;
extern const int LOCAL_PREFIX_LEN;
extern const int ROACH_PREFIX_LEN;
extern const int GSOBS_PREFIX_LEN;

extern bool is_obs_protocol(const char* filename);
// reject_limit is set as unlimited
//
#define REJECT_UNLIMITED -1

extern ErrorCacheEntry* GetForeignErrCacheEntry(Oid relid, uint32 distSessionKey);
List* CNSchedulingForDistOBSFt(Oid foreignTableId);

#endif
