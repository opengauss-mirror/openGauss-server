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
 * streamProducer.h
 *     Support methods for class streamProducer.
 *  
 * IDENTIFICATION
 *     src/include/distributelayer/streamProducer.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_DISTRIBUTELAYER_STREAMPRODUCER_H_
#define SRC_INCLUDE_DISTRIBUTELAYER_STREAMPRODUCER_H_

#include "distributelayer/streamCore.h"

#define VALUE_TYPE 0
#define NUMERIC_TYPE 1
#define VARLENA_TYPE 2
#define CSTRING_TYPE 3
#define TID_TYPE 4
#define FIXED_TYPE 5
#define NAME_TYPE 6

class StreamProducer : public StreamObj {
public:
    StreamProducer(StreamKey key, PlannedStmt* pstmt, Stream* streamNode, MemoryContext context, int socketNum,
        StreamTransType type);

    ~StreamProducer();

    /* Init the stream producer. */
    void init(TupleDesc desc, StreamTxnContext txnCxt, ParamListInfo params, int parentPlanNodeId);

    /* Init stream key */
    void initStreamKey();

    /* Init dataskew info. */
    void initSkewState();

    typedef void (StreamProducer::*criticalSectionFunc)(void);

    /* Connect consumer. */
    void connectConsumer(libcomm_addrinfo** consumerAddr, int& count, int totalNum);

    /* Enter critical section for net protect purpose. */
    void enterCriticalSection(criticalSectionFunc func);

    /* Deinit the producer. */
    void deInit(StreamObjStatus status);

    /* Init the net environment. */
    void netInit();

    /* Init the net port. */
    void netPortInit();

    /* Release net port with protect. */
    void releaseUninitializeResourceWithProtect();

    /* Release net port of sub consumers. */
    void releaseSubConsumerList();

    /* Release net port if net is not init yet. */
    void releaseUninitializeResource();

    /* Register producer thread into thread node group, it should be call in the stream thread. */
    void registerGroup();

    void SetDest(bool is_vec_plan);

    /* Switch the send direction to the nth channel. */
    bool netSwitchDest(int nthChannel);

    /* Save some network status for next sending. */
    void netStatusSave(int nthChannel);

    /*======== Data stream function for tuple ============*/
    /* Broadcast the tuple. */
    void broadCastStream(TupleTableSlot* tuple, DestReceiver* self);

    /* Local broadcast the tuple through memory. */
    void localBroadCastStream(TupleTableSlot* tuple);

    /* Redistribute the tuple. */
    void redistributeStream(TupleTableSlot* tuple, DestReceiver* self);

    /* Local redistribute the tuple through memory. */
    void localRedistributeStream(TupleTableSlot* tuple);

    void redistributeSkewStream(TupleTableSlot* tuple, DestReceiver* self);

    /* Send tuple with Roundrobin. */
    void roundRobinStream(TupleTableSlot* tuple, DestReceiver* self);

#ifdef USE_SPQ
    /* Send batch with Roundrobin. */
    void roundRobinStream(VectorBatch* batch);
    void dmlStream(TupleTableSlot* tuple, DestReceiver* self);
#endif

    /* Local roundrobin the tuple through memory. */
    void localRoundRobinStream(TupleTableSlot* tuple);

    /* Hybrid Stream Type. */
    void hybridStream(TupleTableSlot* tuple, DestReceiver* self);

    /*======== Data stream function for batch ============*/
    /* Broadcast the batch without compress. */
    void broadCastStream(VectorBatch* batch);

    /* Broadcast the batch with compress. */
    void broadCastStreamCompress(VectorBatch* batch);

    /* Local broadcast the batch throug memory. */
    void localBroadCastStream(VectorBatch* batch);

    /* Redistribute the batch. */
    void redistributeStream(VectorBatch* batch);

    /* Mark the data type of each column of a batch. */
    void serializeStreamTypeInit();

    /*Serialize the given row of the batch into the buffer */
    void serializeStream(VectorBatch* batch, int index);

    /* Local redistribute the batch through memory. */
    void localRedistributeStream(VectorBatch* batch);

    /* Send tuple batch Roundrobin. */
    template <BatchCompressType ctype>
    void roundRobinBatch(VectorBatch* batch);

    /* Local roundrobin the batch through memory. */
    void localRoundRobinStream(VectorBatch* batch);

    /* Hybrid Stream Type. */
    void hybridStream(VectorBatch* batch, DestReceiver* self);

    /* Flush the tuple buffer to the destination. */
    void sendByteStream(int nthChannel);

    /* Copy the batch/tuple to shared memory. */
    void sendByMemory(TupleTableSlot* tuple, VectorBatch* batchSrc, int nthChannel, int nthRow = -1);

    /* Mark local stream as finished. */
    void finalizeLocalStream();

    /* Init data buffer in shared context. */
    void initSharedContext();

    /* Flush the data in the buffer. */
    void flushStream();

    /* Set up the write transaction status for the stream thread. */
    void setUpStreamTxnEnvironment();

    /* Report error to consumer node. */
    void reportError();

    /* Report notice to consumer node. */
    void reportNotice();

    /* Get the dummy attribute. */
    bool isDummy();

    /* Get the stream smp distriType. */
    bool isLocalStream();

    /* Copy snapshot. */
    void copySnapShot();

    /* Wait thread ID ready. */
    void waitThreadIdReady();

public:
    /* All get function. */

    /* Get database name. */
    char* getDbName();

    /* Get user name. */
    char* getUserName();

    /* Get the wlm params. */
    WLMGeneralParam getWlmParams();

    uint32 getExplainThreadid();

    unsigned char getExplainTrack();

    /* Get the serialize plan. */
    PlannedStmt* getPlan();

    /* Get the snap shot data. */
    Snapshot getSnapShot();

    /* Get the params. */
    ParamListInfo getParams();

    /* Get the command dest. */
    CommandDest getDest();

    /* Get the instrumentation stream. */
    StreamInstrumentation* getStreamInstrumentation();

    /* Get the OBS instrumentation. */
    OBSInstrumentation* getOBSInstrumentation();

    /* Get the Session Memory structure. */
    SessionLevelMemory* getSessionMemory();

    /* Get child slot. */
    int getChildSlot();

    /* Get parent session id. */
    uint64 getParentSessionid();

    /* Get parent Plannode id. */
    int getParentPlanNodeId();

    /* Get sub producer list. */
    List* getSubProducerList();

    /* Get the value of sync_guc_variables. */
    struct config_generic** get_sync_guc_variables();

    /* Get the memory shared context. */
    StreamSharedContext* getSharedContext();

    int getNth();

    /* Get thread init flag. */
    bool getThreadInit()
    {
        return m_threadInit;
    }

    void getUniqueSQLKey(uint64* unique_id, Oid* user_id, uint32* cn_id);

    /* All set function. */

    /* Get/set postmaster child slot. */
    void setChildSlot(int childSlot);

    /* Set sub producer list. */
    void setSubProducerList(List* subProducerList);

    /* Set sub consumer list. */
    void setSubConsumerList(List* subProducerList);

    /* Set parent session id. */
    void setParentSessionid(uint64 sessionid);

    /* Set the Session Memory structure. */
    void setSessionMemory(SessionLevelMemory* sessionMemory);

    /* Set shared context for in-memory data exchange. */
    void setSharedContext(StreamSharedContext* sharedContext);

    /* Set thread init flag. */
    void setThreadInit(bool flag)
    {
        m_threadInit = flag;
    }

    /* save expr context to producer. */
    void setEcontext(ExprContext* econtext);

    void setUniqueSQLKey(uint64 unique_sql_id, Oid unique_user_id, uint32 unique_cn_id);
    void setGlobalSessionId(GlobalSessionId* globalSessionId);
    void getGlobalSessionId(GlobalSessionId* globalSessionId);

    /* The plan the producer thread will run. */
    PlannedStmt* m_plan;

    /* Database name. */
    char* m_databaseName;

    /* User name. */
    char* m_userName;

    /* Session memory. */
    SessionLevelMemory* m_sessionMemory;

    /* A list to remember the sub producer. */
    List* m_subProducerList;

    /* Transaction context the producer thread will hold. */
    StreamTxnContext m_streamTxnCxt;

    /* Instrumentation data. */
    StreamInstrumentation* m_instrStream;

    /* OBS Instrumentation data */
    OBSInstrumentation* m_obsinstr;

    /* Carry the sync_guc_variables to stream thread. */
    struct config_generic** m_sync_guc_variables;

    ParamListInfo m_params;

    StreamSharedContext* m_sharedContext;

    /*
     * MPP recursive execution support
     *
     * Record the orign nodelist in case of DN pruned in with-recursive execution
     * mode, because the global plan step synchronization we need each datanode's
     * produer start up without short-cut at its execution time.
     */
    /*
     * Store the original corresponding consumer side's nodelist, normally used to
     * send redis/broacast target
     */
    List* m_originConsumerNodeList;

    /*
     * Store the original corresponding consumer side's nodelist, norammly used to
     * respecify the execution stop flag
     */
    List* m_originProducerExecNodeList;

    StreamSyncParam m_syncParam;

private:
    /* Set distribute Idx. */
    void setDistributeIdx();

    /* Set distribute info. */
    void setDistributeInfo();

    /* Two different major redistribute function for tuple. */
    void (StreamProducer::*m_channelCalFun)(TupleTableSlot* tuple);

    /* Vector redistribute function for batch. */
    void (StreamProducer::*m_channelCalVecFun)(VectorBatch* batch);

    /* Choose which node to send by hash value. */
    inline uint2 NodeLocalizer(ScalarValue hashValue);

    /* Choose which thread to send by hash value. */
    inline int ThreadLocalizer(ScalarValue hashValue, int dop);

    template<int keyNum, int distrType>
    void redistributeBatchChannel(VectorBatch* batch);

    template<int keyNum, int distrType>
    void redistributeTupleChannel(TupleTableSlot* tuple);

    template<int distrType>
    void redistributeTupleChannelWithExpr(TupleTableSlot* tuple);

    /* Choose which channel to send by hash value. */
    template<int distrType>
    inline int ChannelLocalizer(ScalarValue hashValue, int Dop, int nodeSize);

    /* Building binding function. */
    template<bool vectorized>
    void BindingRedisFunction();

    /* Dispatch batch sending function. */
    void DispatchBatchRedistrFunction(int len);

    /* Dispatch tuple sending function. */
    void DispatchRowRedistrFunction(int len);

    /* Dispatch batch sending function by redistribute type. */
    template<int len>
    void DispatchBatchRedistrFunctionByRedisType();

    /* Dispatch tuple sending function by redistribute type. */
    template<int len>
    void DispatchRowRedistrFunctionByRedisType();

    typedef Datum (*hashFun)(Datum value);

    void AddCheckInfo(int nthChannel);

    int findLocalChannel();

    inline uint2 NodeLocalizerForSlice(Const **distValues);

    inline int ThreadLocalizerForSlice(ScalarValue hashValue, int dop) const;

    template<int distrType>
    int ChannelLocalizerForSlice(ScalarValue hashValue, Const **distValues, int dop);

    template<int distrType>
    void redistributeTupleChannelForSlice(TupleTableSlot *tuple);

    void DispatchRowRedistrFunctionForSlice();

    template<int distrType>
    void redistributeBatchChannelForSlice(VectorBatch* batch);

    void DispatchBatchRedistrFunctionForSlice();

private:
    /* Hash function for redistribute. */
    hashFun* m_hashFun;

    /* Wlm params. */
    WLMGeneralParam m_wlmParams;

    uint32 m_explain_thread_id;

    bool m_explain_track;

    /*
     * Producer is dummy means it will
     * do very little thing
     *  1) init the child stream thread
     *  2) will end as quick as possible, send nothing outside.
     */
    bool m_isDummy;

    /* A list to remember the sub consumer */
    List* m_subConsumerList;

    /* Postmaster child slot. */
    int m_postMasterChildSlot;

    /* A unique query id for debug purpose. */
    uint64 m_queryId;

    /* Null flag length of redistribute serialization */
    uint32 m_bitNullLen;

    /* Numeric flag length of redistribute serialization */
    uint32 m_bitNumericLen;

    /* temp buffer for redistribute */
    char* m_tempBuffer;

    /* Mark the data type of each column of a batch. */
    uint32* m_colsType;

    /* Parent session ID. */
    uint64 m_parentSessionid;

    /* Parent planNode ID*/
    int m_parentPlanNodeId;

    /* The tuple description for the output data. */
    TupleDesc m_desc;

    /* The consumer nodes information. */
    ExecNodes* m_consumerNodes;

    /* The stream type. */
    StreamType m_streamType;

    /* The dest type the producer thread will follow. */
    CommandDest m_dest;

    /* The bucket map for redistribute. */
    uint2* m_bucketMap;

    int    m_bucketCnt;

    /* boundary extracted from ExecNodes if redistribute by range/list */
    ExecBoundary* m_sliceBoundary;

    /* A quick node index for distribute. */
    uint2** m_disQuickLocator;

    /* A tuple buffer for send message. */
    StringInfoData m_tupleBuffer;

    /* A tuple buffer for send message and check info . */
    StringInfoData m_tupleBufferWithCheck;

    /* The distribute key information. */
    List* m_distributeKey;

    /* Extract from key, as we can have multicolumn keys culrrently, hold the attribute number for convenient. */
    AttrNumber* m_distributeIdx;

    /* Net is init? */
    volatile bool m_netInit;

    /* Use this to protect the net init flag, we can't use spinlock as it may hit the stuck problem. */
    volatile sig_atomic_t m_netProtect;

    int m_roundRobinIdx;

    int m_locator[BatchMaxSize];

    /* The send dest for local stream to send error message. */
    int m_nth;

    bool m_sharedContextInit;
    int64 m_broadcastSize;

    void* m_skewState;

    int m_skewMatch[BatchMaxSize];

    /* If thread already inited*/
    bool m_threadInit;

    /* instrumentation: */
    uint64 m_uniqueSQLId;
    Oid m_uniqueSQLUserId;
    uint32 m_uniqueSQLCNId;

    /* global session id */
    GlobalSessionId m_globalSessionId;

    bool m_hasExprKey;
    List* m_exprkeystate;
    ExprContext* m_econtext;
};

extern THR_LOCAL StreamProducer* streamProducer;

#endif /* SRC_INCLUDE_DISTRIBUTELAYER_STREAMPRODUCER_H_ */
