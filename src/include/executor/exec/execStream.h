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
 * execStream.h
 *        Support routines for datanodes exchange.
 * 
 * 
 * IDENTIFICATION
 *        src/include/executor/execStream.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef EXECSTREAM_H
#define EXECSTREAM_H

#include "pgxc/execRemote.h"
#include "optimizer/planner.h"
#include "optimizer/streamplan.h"
#include "libcomm/libcomm.h"
#include "libpq/libpq-be.h"
#include "access/xact.h"
#include "utils/distribute_test.h"
#include <signal.h>
#include "storage/spin.h"
#include "workload/workload.h"
#include "pgstat.h"
#include "distributelayer/streamConsumer.h"
#include "distributelayer/streamProducer.h"

typedef struct StreamDataBuf {
    char* msg;
    int size;
    int len;
} StreamDataBuf;

#define SetCheckInfo(checkInfo, plan)                                         \
    do {                                                                      \
        *(checkInfo.parentProducerExecNodeList) = plan->exec_nodes->nodeList; \
        *(checkInfo.parentPlanNodeId) = plan->plan_node_id;                   \
        *(checkInfo.parentProducerDop) = plan->dop;                           \
    } while (0)

typedef struct StreamFlowCheckInfo {
    List* parentProducerExecNodeList; /* Producer exec_nodes of parent stream node. */
    int parentPlanNodeId;             /* plan node id of parent node. */
    int parentProducerDop;            /* Dop of parent producer. */
} StreamFlowCheckInfo;

typedef struct StreamFlowCtl {
    PlannedStmt* pstmt;            /* PlannedStmt node, output of planner,
                                   holds the "one time" information needed by the executor. */
    Plan* plan;                    /* Top node of plan tree. */
    List** allConsumerList;        /* All consumer object list. */
    List** allProducerList;        /* All producer object list. */
    List** subProducerList;        /* All sub producer object list. */
    List** subConsumerList;        /* All sub consumer object list. */
    int* threadNum;                /* The number of stream thread. */
    bool dummyThread;              /* If it's a dummy stream node? */
    StreamFlowCheckInfo checkInfo; /* Check if consumer match producer between stream node. */
} StreamFlowCtl;

typedef union {
    struct {
        struct pollfd* ufds; /* tcp poll fds. */
        int* poll2conn;      /* poll idx to connection idx. */
    } tcpLayer;

    struct {
        int* datamarks; /* producer number triggers poll. */
        int* poll2conn; /* poll idx to connection idx. */
        gsocket* gs_sock;
    } sctpLayer;

} StreamNetCtlLayer;

typedef struct StreamNetCtl {
    StreamNetCtlLayer layer; /* stream net controller layer. */
} StreamNetCtl;

typedef struct StreamState {
    ScanState ss;                           /* its first field is NodeTag */
    PGXCNodeHandle** connections;           /* Datanode connections */
    int conn_count;                         /* count of active connections */
    int last_conn_idx;                      /* last connection index */
    bool isReady;                           /* is ready for scan from socket fd*/
    bool need_fresh_data;                   /* need retrieve data from connection */
    int errorCode;                          /* error code to send back to client */
    char* errorMessage;                     /* error message to send back to client */
    char* errorDetail;                      /* error detail to send back to client */
    char* errorContext;                     /* error context to send back to client */
    bool vector_output;                     /* is vector format output */
    StreamDataBuf buf;                      /* data buffer for stream data */
    StreamConsumer* consumer;               /* the stream consumer object which control the stream node*/
    scanStreamFun StreamScan;               /* scan function for tuple stream */
    deserializeStreamFun StreamDeserialize; /* stream deserialize for tuple */
    RemoteErrorData remoteErrData;          /* error data from remote */
    int64 recvDataLen;                      /* network data length for performance info of stream node. */
    StreamNetCtl netctl;                    /* stream net controller for node receiving. */
    StreamSharedContext* sharedContext;     /* Shared context for local stream. */
    TupleVector* tempTupleVec;
    StreamType type; /* indicate it's Redistribute/Broadcast. */
    int64* spill_size;
    void* sortstate;      /* merge sort for stream */
    bool receive_message; /* The stream consumer has receive message from then producer */
#ifdef USE_SPQ
    bool skip_direct_distribute_result;
#endif
} StreamState;

extern StreamState* ExecInitStream(Stream* node, EState* estate, int eflags);
extern void ExecEndStream(StreamState* node);
extern int HandleStreamResponse(PGXCNodeHandle* conn, StreamState* node);
extern void StreamPrepareRequest(StreamState* node);
extern void StreamPrepareRequestForRecursive(StreamState* node, bool stepsync);
extern void StreamReportError(StreamState* node);
extern void ExecEarlyDeinitConsumer(PlanState* node);
extern void SetupStreamRuntime(StreamState* node);
extern bool ScanStreamByLibcomm(StreamState* node);
extern void InitStreamContext();
extern void DeinitStreamContext();
extern void gs_memory_init_entry(StreamSharedContext* sharedContext, int consumerNum, int producerNum);
extern bool ScanMemoryStream(StreamState* node);
extern bool gs_memory_recv(StreamState* node);
extern void gs_memory_send(
    TupleTableSlot* tuple, VectorBatch* batchsrc, StreamSharedContext* sharedContext, int nthChannel, int nthRow);
extern void gs_memory_disconnect(StreamSharedContext* sharedContext, int nthChannel);
extern void gs_message_by_memory(StringInfo buf, StreamSharedContext* sharedContext, int nthChannel);
extern void gs_memory_send_finish(StreamSharedContext* sharedContext, int connNum);
extern void gs_memory_close_conn(StreamSharedContext* sharedContext, int connNum, int smpId);
extern void HandleStreamNotice(StreamState* node, char* msg_body, size_t len);
extern void HandleStreamError(StreamState* node, char* msg_body, int len);
extern const char* GetStreamType(Stream* node);
extern bool ThreadIsDummy(Plan* plan_tree);
extern void AddCheckMessage(StringInfo msg_new, StringInfo msg_org, bool is_stream, unsigned int planNodeId = 0);
extern void CheckMessages(uint64 query_id, uint32 plan_node_id, char* msg, int msg_len, bool is_stream);
extern bool IsThreadProcessStreamRecursive();
extern void ExecReSetStream(StreamState* node);

#endif
