/* ---------------------------------------------------------------------------------------
 * 
 * build_query.h
 *        interfaces used by cm_ctl and gs_ctl to display build progress
 *
 * Portions Copyright (c) 2012-2015, Huawei Tech. Co., Ltd.
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * 
 * IDENTIFICATION
 *        src/include/common/build_query/build_query.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef BUILD_QUERY_H
#define BUILD_QUERY_H

#include "c.h"
#include "replication/replicainternal.h"

#define KB_PER_MB (1024)
#define KB_PER_GB (1024 * 1024)
#define KB_PER_TB (1024 * 1024 * 1024)

#define S_PER_MIN 60
#define S_PER_H (60 * 60)

#define REPORT_TIMEOUT 30   /* report and calculate sync speed every 30s */
#define CACULATE_MIN_TIME 2 /* calculate sync speed at least every 2s */

extern char* show_estimated_time(int time);
extern char* show_datasize(uint64 size);
extern void UpdateDBStateFile(char* path, GaussState* state);

#endif

