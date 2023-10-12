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
 * streamConsumer.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/distributelayer/streamConsumer.h
 *
 * ---------------------------------------------------------------------------------------
 */


#ifndef SRC_INCLUDE_DISTRIBUTELAYER_STREAMCONSUMER_H_
#define SRC_INCLUDE_DISTRIBUTELAYER_STREAMCONSUMER_H_

#include "distributelayer/streamCore.h"

class StreamConsumer;
typedef struct {
    StreamConsumer* consumer;
    int connNum;
    int connInfoSize;
    StreamConnInfo* connInfo;
} StreamValue;

/* Element in stream information table. */
typedef struct {
    StreamKey key;
    StreamValue* value;
} StreamElement;

class StreamConsumer : public StreamObj {
public:
    StreamConsumer(MemoryContext context);

    ~StreamConsumer();

    /* Init the consumer object. */
    void init(StreamKey key, List* execProducerNodes, ParallelDesc desc, StreamTransType transType,
        StreamSharedContext* sharedContext);

    /* Deinit the consumer. */
    void deInit();

    /* Waiting for the producer is ready. */
    void waitProducerReady();

    /* Find un connect producer. */
    void findUnconnectProducer(StringInfo str);

    /* Get nodeIdx of producer which is waited for. */
    int getFirstUnconnectedProducerNodeIdx();

    /* Wake up consumer and let it work. */
    static bool wakeUpConsumerCallBack(CommStreamKey commKey, StreamConnInfo connInfo);

    /* Release comm mode stream. */
    void releaseCommStream();

    /* Get nodeIdx of producer by nodename. */
    int getNodeIdx(const char* nodename);

#ifdef USE_SPQ
    /* Get expectProducer nodeName */
    char* getExpectProducerNodeName();
    void setPstmt(PlannedStmt* p_stmt);
#endif

    /* Get shared context for local stream. */
    inline StreamSharedContext* getSharedContext()
    {
        return m_sharedContext;
    }

    /*
     * Record the orign node list in case of DN pruned in with-recursive execution
     * mode, store the corresponding producer connection list from which the
     * ExecStream will get
     */
    List* m_originProducerNodeList;

private:

    /* Update the stream info. */
    bool updateStreamCommInfo(StreamConnInfo* connInfo);

    /* Update the transport info. */
    void updateTransportInfo(StreamValue* val);

private:
#ifdef USE_SPQ
    PlannedStmt* m_plan;
#endif
    /* Current producer number. */
    int m_currentProducerNum;

    /* All producer is ready or not. */
    bool m_ready;

    /* Expected producer info. */
    StreamConnInfo* m_expectProducer;

    /* Mutex and condition waiting for producer is ready. */
    pthread_mutex_t m_mutex;

    pthread_cond_t m_cond;

    StreamSharedContext* m_sharedContext;
};

#endif /* SRC_INCLUDE_DISTRIBUTELAYER_STREAMCONSUMER_H_ */
