/*
 * Copyright (c) 2023 China Unicom Co.,Ltd.
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
 * gs_uwal.h
 *
 *
 * IDENTIFICATION
 *    src/include/storage/gs_uwal/gs_uwal.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __GS_UWAL_H__
#define __GS_UWAL_H__

#include <cstdlib>
#include <cstdint>
#include "knl/knl_thread.h"
#include "knl/knl_session.h"
#include "replication/walsender_private.h"
#include "replication/walreceiver.h"
#include <access/xact.h>
#include <sys/signalfd.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>

#define UWAL_IP_LEN 16
#define UWAL_PROTOCOL_LEN 16

#define UWAL_CPU_BIND_NUM_DEF 3
#define UWAL_CPU_BIND_NUM_MIN 2
#define UWAL_CPU_BIND_NUM_MAX 16
#define UWAL_CPU_BIND_START_DEF 1
#define UWAL_CPU_BIND_START_MIN 0
#define UWAL_CPU_BIND_START_MAX 1024
#define UWAL_PORT_MAX 65535
#define UWAL_PORT_MIN 9000

const Size MaxReadUwalBytes = 2097152;

typedef struct CBParams {
    pthread_mutex_t  mutex;
    pthread_cond_t cond;
    bool cbResult;
    int ret;
} CBParams;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int opCount;
    int curCount;
    int ret;
    bool interrupted;
    int interruptCount;
} UwalAsynCommonCbCtx;

typedef struct {
    UwalNodeInfo *infos;
    int ret;
    uint64_t writeOffset;
    uint64_t writeLen;
    int index;
    bool processed;
    char *buf;
} UwalAsyncCbCtx;

typedef struct {
    int id;
    char ip[UWAL_IP_LEN];
    int port;
    char protocol[UWAL_PROTOCOL_LEN];
    bool bindCpuSwitch;
    int bindCpuNum;
    int bindCpuStart;
    char repliNodes[MAX_NODE_NUM][UWAL_PROTOCOL_LEN];
} UwalConfig;

int gs_uwal_load_symbols();
/**
 * must called after SetHaShmemData
 */
int GsUwalInit(ServerMode serverMode);
void GetLocalStateInfo(OUT NodeStateInfo* nodeStateInfo);
void GsUwalLogNotifyNodeid (const NodeStateList * nodeStateList);
void GsUwalNotifyCallback(void *ctx, int ret);
int GsUwalSyncNotify(NodeStateList *nodeList);
void GsUwalRcvStateUpdate(XLogRecPtr lastWrited);
UwalrcvWriterState *GsGetCurrentUwalRcvState(void);
void GsUwalRcvFlush();

/**
 * must called in postmaster thread
 * @return
 */
int GsUwalPrimaryInitNotify();
bool FindSyncRepConfig(IN const char *applicationName, OUT int *group, OUT uint8 *syncrepMethod, OUT unsigned *numSync);
/**
 * must called in walsender thread
 * @return
 */
int GsUwalWalSenderNotify(bool exceptSelf = false);
/**
 * must called in walreceiver thread, after connected to primary.
 * @return
 */
int GsUwalWalReceiverNotify(bool isConnectedToPrimary = true);
int GsUwalStandbyInitNotify();

/**
 * called after uwal append
 * @param lsn write log success lsn
 */
void GsUwalUpdateSenderSyncLsn(XLogRecPtr lsn);
UwalVector *GsUwalGetInitInfo();
int GsUwalQueryByUser(TimeLineID ThisTimeLineID, bool needHistoryList = true);
int GsUwalQuery(UwalId *id, UwalBaseInfo *info);
int GsUwalCreate(uint64_t startOffset);
int GsUwalRead(UwalId *id, XLogRecPtr targetPagePtr, char *readBuf, uint64_t readlen);
int GsUwalWriteResHandle(int ret, int nBytes, char *buf, UwalNodeInfo *infos, uint64_t targetOffset);
int GsUwalWrite(int nBytes, char *buf, uint64_t writeOffset);
int GsUwalWriteSync(int nBytes, char *buf, UwalNodeInfo *infos, bool specified = false, uint64_t targetOffset = 0);
void GsUwalWriteAsyncCallBack(void *cbCtx, int retCode);
int GsUwalWriteAsync(int nBytes, char *buf, uint64_t targetOffset);
int GsUwalTruncate(UwalId *id, uint64_t offset);
void GsUwalRenewFileRenamePtr();
bool GsUwalCheckFileRename(XLogRecPtr targetPtr);
int GsUwalRewind(UwalId *id, uint64_t offset);
bool GsUwalCheckLocalRes(UwalNodeInfo *infos);

#endif