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
 * sumStream.cpp
 *
 * IDENTIFICATION
 *    src/common/backend/pgxc_single/stream/sumStream.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "distributelayer/streamProducer.h"
#include "distributelayer/streamConsumer.h"
#include "distributelayer/streamCore.h"

StreamProducer::StreamProducer(
    StreamKey key, PlannedStmt* pstmt, Stream* streamNode, MemoryContext context, int socketNum, StreamTransType type)
    : StreamObj(context, STREAM_PRODUCER)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

StreamProducer::~StreamProducer()
{}

SessionLevelMemory* StreamProducer::getSessionMemory()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void StreamProducer::reportError()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::waitThreadIdReady()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::reportNotice()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

int StreamProducer::getNth()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

StreamSharedContext* StreamProducer::getSharedContext()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

int StreamProducer::getParentPlanNodeId()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

PlannedStmt* StreamProducer::getPlan()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

struct config_generic** StreamProducer::get_sync_guc_variables()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

int StreamProducer::getChildSlot()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

bool StreamProducer::isDummy()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

CommandDest StreamProducer::getDest()

{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();

    return CommandDest();
}

uint64 StreamProducer::getParentSessionid()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

StreamInstrumentation* StreamProducer::getStreamInstrumentation()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

OBSInstrumentation* StreamProducer::getOBSInstrumentation()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

Snapshot StreamProducer::getSnapShot()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

ParamListInfo StreamProducer::getParams()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

WLMGeneralParam StreamProducer::getWlmParams()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return WLMGeneralParam();
}

uint32 StreamProducer::getExplainThreadid()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

unsigned char StreamProducer::getExplainTrack()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

uint64 StreamProducer::getUnqiueSQLId()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

char* StreamProducer::getDbName()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

char* StreamProducer::getUserName()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void StreamProducer::netInit()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::flushStream()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::broadCastStream(TupleTableSlot* tuple, DestReceiver* self)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::localBroadCastStream(TupleTableSlot* tuple)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::redistributeStream(TupleTableSlot* tuple, DestReceiver* self)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::localRedistributeStream(TupleTableSlot* tuple)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::localRoundRobinStream(TupleTableSlot* tuple)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::hybridStream(TupleTableSlot* tuple, DestReceiver* self)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::localBroadCastStream(VectorBatch* batch)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::broadCastStreamCompress(VectorBatch* batch)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::redistributeStream(VectorBatch* batch)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::localRedistributeStream(VectorBatch* batch)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::localRoundRobinStream(VectorBatch* batch)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::hybridStream(VectorBatch* batch, DestReceiver* self)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::finalizeLocalStream()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool StreamProducer::netSwitchDest(int nthChannel)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void StreamProducer::netStatusSave(int nthChannel)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setSharedContext(StreamSharedContext* sharedContext)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setUnqiueSQLId(uint64 unique_sql_id)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setSubProducerList(List* subProducerList)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setSubConsumerList(List* subProducerList)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setSessionMemory(SessionLevelMemory* sessionMemory)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool StreamProducer::isLocalStream()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

void StreamProducer::connectConsumer(sctpaddrinfo** consumerAddr, int& count, int totalNum)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::init(TupleDesc desc, StreamTxnContext txnCxt, ParamListInfo params, int parentPlanNodeId)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setChildSlot(int childSlot)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setParentSessionid(uint64 sessionid)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::setUpStreamTxnEnvironment()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamProducer::initSkewState()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

StreamConsumer::StreamConsumer(MemoryContext context) : StreamObj(context, STREAM_CONSUMER)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

StreamConsumer::~StreamConsumer()
{}

void StreamConsumer::init(StreamKey key, List* execProducerNodes, ParallelDesc desc, StreamTransType transType,
    StreamSharedContext* sharedContext)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamConsumer::waitProducerReady()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

int StreamConsumer::getNodeIdx(const char* nodename)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

void StreamConsumer::deInit()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool StreamConsumer::wakeUpConsumer(StreamKey key, StreamConnInfo connInfo)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

bool StreamConsumer::wakeUpConsumerCallBack(SctpStreamKey key, StreamConnInfo connInfo)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

StreamObj::StreamObj(MemoryContext context, StreamObjType type)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

StreamObj::~StreamObj()
{}

StreamNodeGroup* StreamObj::getNodeGroup()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

StreamTransport** StreamObj::getTransport()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

ParallelDesc StreamObj::getParallelDesc()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return ParallelDesc();
}

StreamKey StreamObj::getKey()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return StreamKey();
}

void StreamObj::setPair(StreamPair* pair)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

int StreamObj::getConnNum()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

void StreamObj::startUp()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

int StreamObj::getPgxcNodeId()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

int StreamObj::getNodeGroupIdx()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

StreamNodeGroup::StreamNodeGroup()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

StreamNodeGroup::~StreamNodeGroup()
{}

bool StreamNodeGroup::inNodeGroup(ThreadId pid1, ThreadId pid2)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

void StreamNodeGroup::SyncConsumerNextPlanStep(int controller_plannodeid, int step)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool StreamNodeGroup::IsRecursiveUnionDone(RecursiveUnionState* state)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

SyncController* StreamNodeGroup::GetSyncController(int controller_plannodeid)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void StreamNodeGroup::AddSyncController(SyncController* controller)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::ConsumerGetSyncUpMessage(
    RecursiveUnionController* controller, int step, StreamState* node, char msg_type)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::ProducerSendSyncMessage(
    RecursiveUnionController* controller, int producer_plannodeid, int step, int tuple_count, char msg_type)

{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::ProducerFinishRUIteration(int step)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

bool StreamNodeGroup::IsRUCoordNode()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

StreamPair* StreamNodeGroup::pushStreamPair(StreamKey key, List* producer, List* consumer)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void StreamNodeGroup::Init(int threadNum)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

int StreamNodeGroup::registerStream(StreamObj* obj)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

StreamPair* StreamNodeGroup::popStreamPair(StreamKey key)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void StreamNodeGroup::MarkRecursiveVfdInvalid()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::initStreamThread(StreamObj* producer, uint8 smpIdentifier, StreamPair* pair)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::ConsumerMarkRUStepStatus(RecursiveUnionController* controller, int step, int iteration,
    StreamState* node, int pgxc_nodeid, int producer_plannodeid, int tuple_processed)

{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::stopAllThreadInNodeGroup(ThreadId pid, uint64 query_id)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::SyncProducerNextPlanStep(int controller_plannodeid, int producer_plannodeid, int step,
    int tuple_count, bool* need_rescan, int target_iteration)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::ConsumerNodeStreamMessage(StreamState* node)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::StartUp()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::setStopFlagPoint(int groupIdx, bool* stopFlag)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::syncQuit(StreamObjStatus status)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::destroy(StreamObjStatus status)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::signalStreamThreadInNodeGroup(int signo)
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::cancelStreamThread()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void StreamNodeGroup::grantStreamConnectPermission()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

ErrorData* StreamNodeGroup::getProducerEdata()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void StreamNodeGroup::restoreStreamEnter()
{
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}
