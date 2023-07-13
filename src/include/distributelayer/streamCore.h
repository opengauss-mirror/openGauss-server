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
 * streamCore.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/distributelayer/streamCore.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef SRC_INCLUDE_DISTRIBUTELAYER_STREAMCORE_H_
#define SRC_INCLUDE_DISTRIBUTELAYER_STREAMCORE_H_

#include <signal.h>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "pgxc/execRemote.h"
#include "optimizer/planner.h"
#include "optimizer/streamplan.h"
#include "libcomm/libcomm.h"
#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "access/xact.h"
#include "utils/distribute_test.h"
#include <signal.h>
#include "storage/spin.h"
#include "workload/workload.h"
#include "pgstat.h"
#include "distributelayer/streamTransportCore.h"

/* The max stream thread in one node. */
#define MAX_STREAM_PER_PLAN 128

/* The max number of nodes that stream can connect. */
#define INIT_STREAMINFO_SIZE 64

#define STREAM_BUF_INIT_SIZE 16 * 1024

#define PRINTTRUE(A) (((A) == true) ? "true" : "false")

#define TupleVectorMaxSize 100

struct StreamState;
class StreamObj;
class StreamNodeGroup;
struct SyncController;

typedef bool (*scanStreamFun)(StreamState* node);
typedef bool (*deserializeStreamFun)(StreamState* node);

typedef struct {
    uint64 queryId; /* u_sess->debug_query_id */
    unsigned int planNodeId;
    unsigned int smpIdentifier;
} StreamKey;

typedef union {
    struct {
        int fd;
    } tcpLayer;

    struct {
        gsocket gsock; /* libcomm logic socket */
    } libcomm_layer;
} StreamIdentity;

typedef struct StreamConnInfo {
    StreamIdentity port;
    char nodeName[NAMEDATALEN];
    Oid nodeoid;
    int nodeIdx;                /* datanode index, like PGXCNodeId */
    unsigned int producerSmpId; /* For SCTP, mark connect which producer */
} StreamConnInfo;

typedef struct {
    ThreadId key;
    StreamNodeGroup* value;
} StreamNodeElement;

typedef struct {
    uint64 key;
} StreamConnectSyncElement;

enum StreamObjType {
    STREAM_PRODUCER,
    STREAM_CONSUMER,
};

enum StreamObjStatus {
    STREAM_UNDEFINED,
    STREAM_INPROGRESS,
    STREAM_COMPLETE,
    STREAM_ERROR,
};

typedef struct {
    StreamObj* streamObj; /* A stream Node has only one or null producer object. */
    List* consumerList;   /* A stream Node may contain many consumer. */
    StreamObjStatus status;
    bool* stopFlag;
} StreamNode;

typedef struct {
    StreamKey key;
    List* producerList;  /* corresponding producer. */
    List* consumerList;  /* corresponding consumer. */
    int expectThreadNum; /* producer thread sub thread number. */
    int createThreadNum;
} StreamPair;

enum DataStatus {
    DATA_EMPTY,
    DATA_PREPARE,
    DATA_READY,
    CONN_ERR,
};

enum EdataWriteStatus {
    EDATA_WRITE_ENABLE,
    EDATA_WRITE_START,
    EDATA_WRITE_FINISH,
    ERROR_READ_FINISH,
};

typedef struct TupleVector {
    TupleTableSlot** tupleVector;
    int tuplePointer;
    int tupleCount;
} TupleVector;

typedef struct StreamSharedContext {
    MemoryContext localStreamMemoryCtx; /**/
    VectorBatch*** sharedBatches;
    TupleVector*** sharedTuples;
    StringInfo** messages;
    DataStatus** dataStatus;
    bool** is_connect_end;
    int* scanLoc;
    TcpStreamKey key_s;
    bool vectorized;
    struct hash_entry** poll_entrys;
    struct hash_entry*** quota_entrys;
} StreamSharedContext;

typedef struct StreamSyncParam {
    Oid TempNamespace;
    Oid TempToastNamespace;
    bool IsBinaryUpgrade;
    bool CommIpcLog;
} StreamSyncParam;

class StreamObj : public BaseObject {
public:
    StreamObj(MemoryContext context, StreamObjType type);
    ~StreamObj();

    /* Initiliaze the stream environment. */
    static void startUp();

    /* Get the node group. */
    StreamNodeGroup* getNodeGroup();

    /* Set stream thread id. */
    void setThreadId(ThreadId pid);

    /* Set stream pair. */
    void setPair(StreamPair* pair);

    /* Get the stream key. */
    StreamKey getKey();

    /* Get the stream type. */
    StreamObjType getType();

    /* Get parallel description. */
    ParallelDesc getParallelDesc();

    /* Get stream thread id. */
    ThreadId getThreadId();

    /* Get the pgxc data node id. */
    int getPgxcNodeId();

    /* Get the index itself in the group. */
    int getNodeGroupIdx();

    /* Get the stream pair. */
    StreamPair* getPair();

    /* Get the stream plan node. */
    const Stream* getStream();

    /* Get stream transport. */
    StreamTransport** getTransport();

    /* Get connection number. */
    int getConnNum();

    /* Release net port. */
    void releaseNetPort();

    /* Release net port in hash table. */
    static void releaseNetPortInHashTable();

    /* Stream node group belongs to. */
    StreamNodeGroup* m_nodeGroup;

    /* Stream transport array. */
    StreamTransport** m_transport;

    /* Stream pair. */
    StreamPair* m_pair;

protected:
    /* Stream type. */
    StreamObjType m_type;

    /* Stream key. */
    StreamKey m_key;

    /* Memory context the stream object use. */
    MemoryContext m_memoryCxt;

    /* Stream transportation type. */
    StreamTransType m_transtype;

    /* Expected socket number. */
    int m_connNum;

    /* Stream node. */
    Stream* m_streamNode;

    /* Identify itself in the group. */
    int m_nodeGroupIdx;

    /* The thread id that take the stream object. */
    ThreadId m_threadId;

    /* The node id that the stream thread is running on. */
    int m_nodeId;

    /* Stream object is init or not. */
    bool m_init;

    /* Stream sync object is init or not. */
    bool m_threadSyncObjInit;

    /* Stream object status. */
    StreamObjStatus m_status;

    /* Parallel description. */
    ParallelDesc m_parallel_desc;

protected:
    /* Global context stream object using. */
    static MemoryContext m_memoryGlobalCxt;

    /* Hash table for stream information, mainly for stream connect. */
    static HTAB* m_streamInfoTbl;

public:
    /* Mutex for stream info table. */
    static pthread_mutex_t m_streamInfoLock;
};

/* Stream node group is book keeper for stream object. */
class StreamNodeGroup : public BaseObject {
public:
    StreamNodeGroup();

    ~StreamNodeGroup();

    /* Init stream node group. */
    void Init(int threadNum);

    /* Init some static member. */
    static void StartUp();

    /* Stop all register stream thread in the node group. */
    static void stopAllThreadInNodeGroup(ThreadId pid, uint64 query_id);

    /* Destory a stream Node group, clean some status. */
    static void destroy(StreamObjStatus status);

    /* Synchronize quit. */
    static void syncQuit(StreamObjStatus status);

    static void ReleaseStreamGroup(bool resetSession, StreamObjStatus status = STREAM_COMPLETE);

    /* Grant stream connect permission. */
    static void grantStreamConnectPermission();

    /* Revoke stream connect permission. */
    static void revokeStreamConnectPermission();

    /* Check if can accept connection. */
    static bool checkStreamConnectPermission(uint64 query_id);

    /* Clear the stream node group. */
    void deInit(StreamObjStatus status);

    /* Register the object in stream node group. */
    int registerStream(StreamObj* obj);

    /* Unregister the stream object represented in group idx with status. */
    void unregisterStream(int groupIdx, StreamObjStatus status);

    /* Send signal to all stream thread registered in node group. */
    void signalStreamThreadInNodeGroup(int signo);

    /* Cancel all stream thread registered in node group. */
    static void cancelStreamThread();

    /* Set the stop flag. */
    void setStopFlagPoint(int groupIdx, bool* stopFlag);

    /* Set the node status. */
    void setNodeStatus(int groupIdx, StreamObjStatus status);

    /* Wait all thread in the node group to quit. */
    void quitSyncPoint();

    /* Push a stream pair. */
    StreamPair* pushStreamPair(StreamKey key, List* producer, List* consumer);

    /* Pop a stream pair according to key. */
    StreamPair* popStreamPair(StreamKey key);

    /* Get stream pair list */
    const List* getStreamPairList();

    /* Start a stream thread. */
    void initStreamThread(StreamProducer* producer, uint8 smpIdentifier, StreamPair* pair);

    /* In the same node group? */
    bool inNodeGroup(ThreadId pid1, ThreadId pid2);

    /* Restore stream enter. */
    void restoreStreamEnter();

    /* Set need clean flag. */
    void setNeedClean(bool flag);

    /* Check if query already canceled. */
    bool isQueryCanceled()
    {
        return m_canceled;
    }

    /* Save the first error data of producer */
    void saveProducerEdata();

    /* Get the saved error data of producer */
    ErrorData* getProducerEdata();

    /* Stream Consumer List. */
    List* m_streamConsumerList;

    /* Stream Producer List. */
    List* m_streamProducerList;

    /* Controller list for recursive */
    List* m_syncControllers;

    MemoryContext m_streamRuntimeContext;

    /* Save the first error data of producer thread */
    ErrorData* m_producerEdata;

    /* MPP with-recursive support */
    static void SyncConsumerNextPlanStep(int controller_plannodeid, int step);
    static void SyncProducerNextPlanStep(int controller_plannodeid, int producer_plannodeid, int step, int tuple_count,
        bool* need_rescan, int target_iteration);

    void ConsumerNodeStreamMessage(StreamState* node);
    void ConsumerGetSyncUpMessage(RecursiveUnionController* controller, int step, StreamState* node, char msg_type);
    void ConsumerMarkRUStepStatus(RecursiveUnionController* controller, int step, int iteration, StreamState* node,
        int pgxc_nodeid, int producer_plannodeid, int tuple_processed);
    void ProducerSendSyncMessage(
        RecursiveUnionController* controller, int producer_plannodeid, int step, int tuple_count, char msg_type);
    void ProducerFinishRUIteration(int step);
    static bool IsRecursiveUnionDone(RecursiveUnionState* state);
    static bool IsRUCoordNode();
    static bool IsRUSyncProducer();
    void AddSyncController(SyncController* controller);
    SyncController* GetSyncController(int controller_plannodeid);
    void MarkSyncControllerStopFlagAll();

    inline pthread_mutex_t* GetStreamMutext()
    {
        return &m_mutex;
    };

    inline pthread_mutex_t* GetRecursiveMutex()
    {
        return &m_recursiveMutex;
    };

    inline uint64 GetQueryId();

    inline void SetRecursiveVfdInvalid()
    {
        m_recursiveVfdInvalid = true;
    }

    inline bool GetRecursiveVfdInvalid()
    {
        return m_recursiveVfdInvalid;
    }
#ifndef ENABLE_MULTIPLE_NODES
    inline void MarkStreamQuitStatus(StreamObjStatus status)
    {
        m_quitStatus = status;
    }

    inline StreamObjStatus GetStreamQuitStatus()
    {
        return m_quitStatus;
    }
    /* Send stop signal to all stream threads in node group. */
    void SigStreamThreadClose();

    struct PortalData *m_portal;
#endif
    /* Mark recursive vfd is invalid before aborting transaction. */
    static void MarkRecursiveVfdInvalid();

private:
    /* Set the executor stop flag to true. */
    void stopThread();

    /* Top consumer pid also the node group creator pid. */
    ThreadId m_pid;

    /* How many stream object can hold. */
    int m_size;

    /* How many stream object currently hold. */
    int m_streamNum;

    /* Stream Object Node Container. */
    StreamNode* m_streamArray;

    /* Stream Pair list. */
    List* m_streamPairList;

    /* A counter that't signal the stream thread group can destroy. */
    volatile int m_quitWaitCond;

    /* A counter remeber the already created thread number. */
    volatile int m_createThreadNum;

    /* A flag to indicate stream enter the quit point. */
    volatile int m_streamEnter;

    /* A counter to indicate stream enter the quit point. */
    volatile int m_streamEnterCount;

    /* Mutex and condition waiting for all thread in the node group is complete. */
    pthread_mutex_t m_mutex;

    pthread_cond_t m_cond;

    /* Mark if query already canceled. */
    bool m_canceled;

    /* Mark if need clean stream info hash table. */
    bool m_needClean;

    /* First error data write protect */
    int m_edataWriteProtect;

    /* Mark if query failed. */
    bool m_errorStop;

    /* Mark if VFD of recursive is invalid. */
    bool m_recursiveVfdInvalid;

    /* Mutex for sync controller and vfd operation. */
    pthread_mutex_t m_recursiveMutex;

    /* Global context stream object using. */
    static MemoryContext m_memoryGlobalCxt;

    /* Hash table for stream node group. */
    static HTAB* m_streamNodeGroupTbl;

    /* Mutex for stream node group. */
    static pthread_mutex_t m_streamNodeGroupLock;

    /* Hash table for stream connect sync. */
    static HTAB* m_streamConnectSyncTbl;

    /* Mutex for stream connect sync. */
    static pthread_mutex_t m_streamConnectSyncLock;
#ifndef ENABLE_MULTIPLE_NODES
    /* Mark Stream query quit status. */
    StreamObjStatus m_quitStatus;
#endif
};

extern bool IsThreadProcessStreamRecursive();
extern bool InitStreamObject(PlannedStmt* planStmt);
#ifndef ENABLE_MULTIPLE_NODES
extern void StreamMarkStop();
#endif
#endif /* SRC_INCLUDE_DISTRIBUTELAYER_STREAMCORE_H_ */
