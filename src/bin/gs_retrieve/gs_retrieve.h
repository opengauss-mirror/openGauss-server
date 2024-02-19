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
 * IDENTIFICATION
 *    src/bin/gs_retrieve/gs_retrieve.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_RETRIEVE_H
#define GS_RETRIEVE_H

#include <set>
#include "postgres_fe.h"
#include "storage/standby.h"

/* command options */
char *dbname = NULL;
char *newHost = NULL;
int newPort = 0;
char *oldHost = NULL;
int oldPort = 0;
char *userName = NULL;
char *decodePlugin = NULL;
char *outputFile = NULL;
bool doAreaChange = false;
char *datadir = NULL;
const char *progname = NULL;
std::set<TransactionId> needDecodedXids;
int dbgetpassword = 0;
int recvTimeout = 120;    /* 120 sec = default */
int connectTimeout = 120;
/* maybe have some xlog dir in enable_dss mode */
char** xlogDirs = NULL;
int xlogDirNum = 0;

typedef struct XLogPageReadPrivate {
    const char *datadir;
    TimeLineID tli;
} XLogPageReadPrivate;

#define DISCONNECT_AND_RETURN_NULL(tempconn) \
    do {                                     \
        if ((tempconn) != NULL) {            \
            PQfinish(tempconn);              \
            tempconn = NULL;                 \
        }                                    \
        (tempconn) = NULL;                   \
        return tempconn;                     \
    } while (0)

#define FCLOSE_AND_RETURN_FALSE(decodeFile)  \
    do {                                     \
        if (strcmp(outputFile, "-") != 0) {  \
            (void)fclose(decodeFile);        \
        }                                    \
        return false;                        \
    } while (0)

#define IS_COMMIT_XLOG(record, xact_info)                                         \
    ((record)->xl_rmid == RM_XACT_ID && (((xact_info) == XLOG_XACT_COMMIT_COMPACT) || \
    ((xact_info) == XLOG_XACT_COMMIT) || ((xact_info) == XLOG_XACT_COMMIT_PREPARED)))

#define MAXCMDLEN 1024

#endif   /* GS_RETRIEVE_H */