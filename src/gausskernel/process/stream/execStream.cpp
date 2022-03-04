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
 * execStream.cpp
 *	  Support routines for datanodes exchange.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/stream/execStream.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <time.h>
#include "postgres.h"
#include "knl/knl_variable.h"

#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "executor/node/nodeRecursiveunion.h"
#include "pgxc/execRemote.h"
#include "nodes/nodes.h"
#include "access/printtup.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libcomm/libcomm.h"
#include "libpq/pqformat.h"
#include <sys/poll.h>
#include "executor/exec/execStream.h"
#include "postmaster/postmaster.h"
#include "access/transam.h"
#include "gssignal/gs_signal.h"
#include "utils/anls_opt.h"
#include "utils/distribute_test.h"
#include "utils/guc_tables.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/combocid.h"
#include "storage/procarray.h"
#include "vecexecutor/vecstream.h"
#include "vecexecutor/vectorbatch.h"
#include "access/hash.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "distributelayer/streamTransportComm.h"
#include "optimizer/nodegroups.h"
#include "optimizer/dataskew.h"
#include "instruments/instr_unique_sql.h"

static void CheckStreamMatchInfo(
    StreamFlowCheckInfo checkInfo, int plan_node_id, List* consumerExecNode, int consumerDop, bool isLocalStream);
static void InitStream(StreamFlowCtl* ctl, StreamTransType type);
static void InitStreamFlow(StreamFlowCtl* ctl);

#define NODENAMELEN 64

bool IsThreadProcessStreamRecursive()
{
    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;

    if (!IS_PGXC_DATANODE || stream_nodegroup == NULL || stream_nodegroup->m_syncControllers == NIL) {
        return false;
    }

    return true;
}

/*
 * @Description: Check if Stream node is dummy
 *
 * @param[IN] plan_tree:  plan tree
 * @return: bool, true if is dummy
 */
bool ThreadIsDummy(Plan* plan_tree)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* For top consumer, we need to check consumer_nodes if top node of plan_tree is Stream. */
    if (IsA(plan_tree, Stream) || IsA(plan_tree, VecStream)) {
        Stream* streamNode = (Stream*)plan_tree;
        List* nodeList = streamNode->consumer_nodes->nodeList;
        return !list_member_int(nodeList, u_sess->pgxc_cxt.PGXCNodeId);
    } else {
        return !list_member_int(plan_tree->exec_nodes->nodeList, u_sess->pgxc_cxt.PGXCNodeId);
    }
#else
    return false;
#endif
}

const char* GetStreamTypeRedistribute(Stream* node)
{
    const char* stream_tag = NULL;
    bool isRangeListRedis = (node->consumer_nodes->boundaries != NULL);
    bool isRangeRedis = (isRangeListRedis) ? 
        (node->consumer_nodes->boundaries->locatorType == LOCATOR_TYPE_RANGE) : false;

    switch (node->smpDesc.distriType) {
        case LOCAL_DISTRIBUTE:
            stream_tag = "LOCAL REDISTRIBUTE";
            break;
    
        case LOCAL_BROADCAST:
            stream_tag = "LOCAL BROADCAST";
            break;
    
        case LOCAL_ROUNDROBIN:
            stream_tag = (node->smpDesc.consumerDop == 1) ? "LOCAL GATHER" : "LOCAL ROUNDROBIN";
            break;
    
        case REMOTE_SPLIT_DISTRIBUTE: {
            if (isRangeListRedis) {
                if (isRangeRedis) {
                    stream_tag = "SPLIT RANGE REDISTRIBUTE";
                } else {
                    stream_tag = "SPLIT LIST REDISTRIBUTE";
                }
            } else {
                stream_tag = "SPLIT REDISTRIBUTE";
            }
            break;
        }

        default: {
            if (isRangeListRedis) {
                if (isRangeRedis) {
                    stream_tag = "RANGE REDISTRIBUTE";
                } else {
                    stream_tag = "LIST REDISTRIBUTE";
                }
            } else {
                stream_tag = "REDISTRIBUTE";
            }
            break;
        }
    }

    return stream_tag;
}

const char* GetStreamTypeHybrid(Stream* node)
{
    const char* stream_tag = NULL;
    List* skew_list = node->skew_list;
    ListCell* lc = NULL;
    SkewStreamType sstype = PART_NONE;
    
    Assert(list_length(skew_list) >= 1);
    
    /* check if all qual skew share the same stream type. */
    foreach (lc, skew_list) {
        QualSkewInfo* qsinfo = (QualSkewInfo*)lfirst(lc);
    
        if (sstype == PART_NONE)
            sstype = qsinfo->skew_stream_type;
    
        if (sstype != qsinfo->skew_stream_type) {
            sstype = PART_NONE;
            break;
        }
    }
    
    switch (sstype) {
        case PART_REDISTRIBUTE_PART_BROADCAST:
            stream_tag = "PART REDISTRIBUTE PART BROADCAST";
            break;
        case PART_REDISTRIBUTE_PART_ROUNDROBIN:
            stream_tag = "PART REDISTRIBUTE PART ROUNDROBIN";
            break;
        case PART_REDISTRIBUTE_PART_LOCAL:
            stream_tag = "PART REDISTRIBUTE PART LOCAL";
            break;
        case PART_LOCAL_PART_BROADCAST:
            stream_tag = "PART LOCAL PART BROADCAST";
            break;
        default:
            stream_tag = "HYBRID";
            break;
    }

    return stream_tag;
}


/*
 * @Description: Emit stream type details
 *
 * @param[IN] node:  stream plan node
 * @return: string of stream type
 */
const char* GetStreamType(Stream* node)
{
    Plan* stream_plan = (Plan*)node;
    const char* vector_tag = stream_plan->vec_output ? "Vector " : "";
    const char* stream_tag = NULL;
    char dop_tag[100] = {0};
    char ng_tag[NODENAMELEN * 2 + 10] = {0};
    StringInfo type = makeStringInfo();
    char* consumerGroupName = NULL;
    char* producerGroupName = NULL;

    /* Set DOP tag if parallel enabled */
    if (node->smpDesc.consumerDop > 1 || node->smpDesc.producerDop > 1) {
        errno_t rc =
            sprintf_s(dop_tag, sizeof(dop_tag), " dop: %d/%d", node->smpDesc.consumerDop, node->smpDesc.producerDop);
        securec_check_ss(rc, "\0", "\0");
    }

    /* show nodegroup shuffle infomation, ng: group1->group2 */
    if (ng_enable_nodegroup_explain()) {
        ExecNodes* consumerNodes = node->consumer_nodes;
        ExecNodes* producerNodes = node->scan.plan.exec_nodes;

        if (!ng_is_same_group(&(consumerNodes->distribution), &(producerNodes->distribution))) {
            consumerGroupName = ng_get_dist_group_name(&(consumerNodes->distribution));
            producerGroupName = ng_get_dist_group_name(&(producerNodes->distribution));

            errno_t rc = sprintf_s(ng_tag, sizeof(ng_tag), " ng: %s->%s", producerGroupName, consumerGroupName);
            securec_check_ss(rc, "\0", "\0");
        }
    }

    switch (node->type) {
        case STREAM_BROADCAST: {
            if (node->smpDesc.distriType == REMOTE_SPLIT_BROADCAST) {
                stream_tag = "SPLIT BROADCAST";
            } else {
                stream_tag = "BROADCAST";
            }

            appendStringInfo(type, "%sStreaming(type: %s%s%s)", vector_tag, stream_tag, dop_tag, ng_tag);
        } break;

        case STREAM_REDISTRIBUTE: {
            stream_tag = GetStreamTypeRedistribute(node);
            appendStringInfo(type, "%sStreaming(type: %s%s%s)", vector_tag, stream_tag, dop_tag, ng_tag);
        } break;

        case STREAM_HYBRID: {
            stream_tag = GetStreamTypeHybrid(node);
            appendStringInfo(type, "%sStreaming(type: %s%s%s)", vector_tag, stream_tag, dop_tag, ng_tag);
        } break;
        default:
            appendStringInfo(type, "UNKNOWN");
            break;
    }

    return type->data;
}

void StreamSaveTxnContext(StreamTxnContext* stc)
{
    StreamTxnContextSaveComboCid(stc);
    StreamTxnContextSaveXact(stc);
    StreamTxnContextSaveSnapmgr(stc);
    StreamTxnContextSaveInvalidMsg(stc);
}

void StreamRestoreTxnContext(StreamTxnContext* stc)
{
    StreamTxnContextRestoreComboCid(stc);
    StreamTxnContextRestoreXact(stc);
    StreamTxnContextRestoreSnapmgr(stc);
    StreamTxnContextRestoreInvalidMsg(stc);
}

/*
 * @Description: Find stream object by smpId
 *
 * @param[IN] streamList:  stream object list
 * @param[IN] smpIdentifier:  smp identifier
 * @return: pointer to stream object
 */
static StreamObj* FindStreamObjBySmpId(List* streamList, uint32 smpIdentifier)
{
    ListCell* cell = NULL;
    StreamObj* obj = NULL;

    foreach (cell, streamList) {
        obj = (StreamObj*)lfirst(cell);
        if (obj->getKey().smpIdentifier == smpIdentifier)
            break;
        else
            obj = NULL;
    }

    return obj;
}

static StreamSharedContext* buildLocalStreamContext(Stream* streamNode, PlannedStmt* pstmt)
{
    if (!STREAM_IS_LOCAL_NODE(streamNode->smpDesc.distriType))
        return NULL;

    StreamSharedContext* sharedContext = (StreamSharedContext*)palloc0(sizeof(StreamSharedContext));
    MemoryContext localStreamMemoryCtx = NULL;
    VectorBatch*** sharedBatches = NULL;
    TupleVector*** sharedTuples = NULL;
    DataStatus** dataStatus = NULL;
    bool** is_connect_end = NULL;
    StringInfo** messages = NULL;
    int* scanLoc = NULL;
    char context_name[NODENAMELEN];
    int rc = 0;
    int consumerNum = streamNode->smpDesc.consumerDop;
    int producerNum = streamNode->smpDesc.producerDop;

    Assert(streamNode->scan.plan.plan_node_id != 0);
    /* Set memory context name. */
    rc = snprintf_s(context_name,
        NODENAMELEN,
        NODENAMELEN - 1,
        "%s_%lu_%d",
        "LocalStreamMemoryContext",
        u_sess->debug_query_id,
        streamNode->scan.plan.plan_node_id);
    securec_check_ss(rc, "", "");

    /* Create a shared memory context for local stream in-memory data exchange. */
    localStreamMemoryCtx = AllocSetContextCreate(u_sess->stream_cxt.data_exchange_mem_cxt,
        context_name,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    MemoryContext oldCxt = MemoryContextSwitchTo(localStreamMemoryCtx);

    scanLoc = (int*)palloc0(sizeof(int) * consumerNum);

    /* Init data status. */
    dataStatus = (DataStatus**)palloc0(sizeof(DataStatus*) * consumerNum);
    is_connect_end = (bool**)palloc0(sizeof(bool*) * consumerNum);
    messages = (StringInfo**)palloc0(sizeof(StringInfo*) * consumerNum);
    for (int i = 0; i < consumerNum; i++) {
        dataStatus[i] = (DataStatus*)palloc0(sizeof(DataStatus) * producerNum);
        is_connect_end[i] = (bool*)palloc0(sizeof(bool) * producerNum);
        messages[i] = (StringInfo*)palloc0(sizeof(StringInfo) * producerNum);
        for (int j = 0; j < producerNum; j++) {
            dataStatus[i][j] = DATA_EMPTY;
            is_connect_end[i][j] = false;
            messages[i][j] = (StringInfoData*)palloc0(sizeof(StringInfoData));
            initStringInfo(messages[i][j]);
        }
    }

    /* Init shared batches or tuples */
    if (IsA((Plan*)streamNode, VecStream)) {
        sharedBatches = (VectorBatch***)palloc0(sizeof(VectorBatch**) * consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            sharedBatches[i] = (VectorBatch**)palloc0(sizeof(VectorBatch*) * producerNum);
        }
    } else {
        sharedTuples = (TupleVector***)palloc0(sizeof(TupleVector**) * consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            sharedTuples[i] = (TupleVector**)palloc0(sizeof(TupleVector*) * producerNum);
        }
    }

    sharedContext->vectorized = IsA(&(streamNode->scan.plan), VecStream);
    sharedContext->localStreamMemoryCtx = localStreamMemoryCtx;
    sharedContext->sharedBatches = sharedBatches;
    sharedContext->sharedTuples = sharedTuples;
    sharedContext->dataStatus = dataStatus;
    sharedContext->is_connect_end = is_connect_end;
    sharedContext->messages = messages;
    sharedContext->scanLoc = scanLoc;

    /* Set stream key. */
    sharedContext->key_s.queryId = pstmt->queryId;
    sharedContext->key_s.planNodeId = streamNode->scan.plan.plan_node_id;
    sharedContext->key_s.producerSmpId = ~0;
    sharedContext->key_s.consumerSmpId = ~0;

    gs_memory_init_entry(sharedContext, consumerNum, producerNum);
    MemoryContextSwitchTo(oldCxt);

    return sharedContext;
}

/*
 * @Description: reset local stream context for LOCAL GATHER of Recursive
 *
 * @param[IN] context: local stream context cached in consumer.
 * @return void
 */
static void resetLocalStreamContext(StreamSharedContext* context)
{
    if (context == NULL)
        return;

    context->scanLoc[0] = 0;
    context->dataStatus[0][0] = DATA_EMPTY;
    context->is_connect_end[0][0] = false;
    resetStringInfo(context->messages[0][0]);

    context->sharedTuples[0][0]->tuplePointer = 0;
}

static RecursiveUnion* GetRecursiveUnionSubPlan(PlannedStmt* pstmt, int subplanid)
{
    ListCell* lc = NULL;
    foreach (lc, pstmt->subplans) {
        Plan* p = (Plan*)lfirst(lc);
        if (IsA(p, RecursiveUnion) && p->plan_node_id == subplanid) {
            return (RecursiveUnion*)p;
        }
    }

    return NULL;
}

/*
 * get one tuple for stream with merge sort
 */
bool StreamMergeSortGetTuple(StreamState* node)
{
    return tuplesort_gettupleslot((Tuplesortstate*)node->sortstate, true, node->ss.ps.ps_ResultTupleSlot, NULL);
}

/*
 * init stream with merge sort
 */
void InitStreamMergeSort(StreamState* node)
{
    Assert(node->StreamScan == ScanStreamByLibcomm);
    Assert(node->sortstate == NULL);
    TupleTableSlot* scanslot = node->ss.ps.ps_ResultTupleSlot;
    SimpleSort* sort = ((Stream*)(node->ss.ps.plan))->sort;
    Assert(sort->sortToStore == false);

    if (node->need_fresh_data) {
        if (datanode_receive_from_logic_conn(node->conn_count, node->connections, &node->netctl, -1)) {
            int error_code = getStreamSocketError(gs_comm_strerror());
            ereport(ERROR,
                (errcode(error_code),
                    errmsg("Failed to read response from Datanodes. Detail: %s\n", gs_comm_strerror())));
        }
    }

    node->sortstate = tuplesort_begin_merge(scanslot->tts_tupleDescriptor,
        sort->numCols,
        sort->sortColIdx,
        sort->sortOperators,
        sort->sortCollations,
        sort->nullsFirst,
        node,
        u_sess->attr.attr_memory.work_mem);

    node->StreamScan = StreamMergeSortGetTuple;
}

/*
 * @Description: Check match info between producer and consumer for stream node
 *
 * @param[IN] checkInfo:  info used to check.
 * @param[IN] plan_node_id:  plan node id of current stream node.
 * @param[IN] consumerExecNode:  consumer exec nodes of current stream node.
 * @param[IN] consumerDop:  consumer dop of current stream node.
 * @param[IN] isLocalStream:  if it`s a local stream node.
 * @return: void
 */
static void CheckStreamMatchInfo(
    StreamFlowCheckInfo checkInfo, int plan_node_id, List* consumerExecNode, int consumerDop, bool isLocalStream)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* 1.Check exec_nodes. */
    if (checkInfo.parentProducerExecNodeList == NIL) {
        ereport(ERROR,
                (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                errmsg("Stream plan check failed. Execution datanodes list of stream node[%d] should never be null.",
                        checkInfo.parentPlanNodeId)));
    }

    /*
     * For local stream node we do not check the exec nodes cause consumer nodes
     * will be modified forcibly or will be marked as dummy thread, so just skipping.
     */
    if (!isLocalStream) {
        if (list_length(checkInfo.parentProducerExecNodeList) != list_length(consumerExecNode))
            ereport(ERROR,
                (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                    errmsg("Stream plan check failed. Execution datanodes list of stream node[%d] mismatch in parent "
                           "node[%d].",
                        plan_node_id,
                        checkInfo.parentPlanNodeId),
                    errhint("Please use EXPLAIN VERBOSE command to see more details.")));

        else {
            List* l_diff1 = list_difference_int(checkInfo.parentProducerExecNodeList, consumerExecNode);
            List* l_diff2 = list_difference_int(consumerExecNode, checkInfo.parentProducerExecNodeList);

            if (l_diff1 != NIL || l_diff2 != NIL) {
                list_free(l_diff1);
                list_free(l_diff2);
                ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("Stream plan check failed. Execution datanodes list of stream node[%d] mismatch in "
                               "parent node[%d].",
                            plan_node_id,
                            checkInfo.parentPlanNodeId),
                        errhint("Please use EXPLAIN VERBOSE command to see more details.")));
            }
        }
    }
#endif
    /* 2.Check dop. */
    if (SET_DOP(checkInfo.parentProducerDop) != SET_DOP(consumerDop))
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                errmsg("Stream plan check failed. Query dop of stream node %d [dop: %d] mismatch in parent node %d "
                       "[dop: %d].",
                    plan_node_id,
                    SET_DOP(consumerDop),
                    checkInfo.parentPlanNodeId,
                    SET_DOP(checkInfo.parentProducerDop)),
                errhint("Please use EXPLAIN VERBOSE command to see more details.")));
}

/*
 * @Description: Create producer and consumer objects for stream node
 *
 * @param[IN] ctl:  controller used to init stream
 * @param[IN] transType:  TCP or SCTP
 * @return: void
 */
static void InitStream(StreamFlowCtl* ctl, StreamTransType transType)
{
    int i = 0;
    StreamKey key;
    StreamConsumer* consumer = NULL;
    StreamProducer* producer = NULL;
    List* execNodes = NULL;
    int consumerNum = 0;
    int producerConnNum = 0;
    PlannedStmt* pstmt = ctl->pstmt;
    Plan* plan = ctl->plan;
    Stream* streamNode = (Stream*)ctl->plan;
    /* May split to mutiliple consumer/producer list, m:n model. */
    List* consumerSMPList = NULL;
    List* producerSMPList = NULL;
    bool isLocalStream = STREAM_IS_LOCAL_NODE(streamNode->smpDesc.distriType);
    /* MPP with-recursive support */
    bool startall = false;
    List* consumer_nodeList = NIL;
    List* producer_nodeList = NIL;

#ifndef ENABLE_MULTIPLE_NODES
    if (!isLocalStream) {
        ereport(ERROR, (errmsg("Single Node should only has local stream operator.")));
    }
#endif

    StreamSharedContext* sharedContext = NULL;
    sharedContext = buildLocalStreamContext(streamNode, pstmt);

    key.queryId = pstmt->queryId;
    key.planNodeId = plan->plan_node_id;

    /*
     * MPPDB with-recursive support
     */
    bool isRecursive = EXEC_IN_RECURSIVE_MODE(plan) && !streamNode->is_recursive_local;
    if (isRecursive) {
        /* If we are under recursive-union, we need check if P/C is not a N-N case */
        Plan* producer_topplan = plan->lefttree;
        RecursiveUnion* ruplan = GetRecursiveUnionSubPlan(pstmt, plan->recursive_union_plan_nodeid);
        if (ruplan == NULL) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("The ruplan is can not be NULL")));
        }
        List* cteplan_nodeList = ruplan->plan.exec_nodes->nodeList;
        consumer_nodeList = streamNode->consumer_nodes->nodeList;
        producer_nodeList = producer_topplan->exec_nodes->nodeList;

        /*
         * Check if the consumer & top-producer's exec nodes is identical with
         * Recursive CTE plan's nodes
         */
        bool isEqcalConsumerAndProducer = (!equal(cteplan_nodeList, consumer_nodeList) ||
                !equal(cteplan_nodeList, producer_nodeList));
        if (isEqcalConsumerAndProducer) {
            /* mark we need start all */
            startall = true;

            /*
             * Reset consumer and producer list, and save the original consumer_nodes
             * with the plan node
             *
             * Note, only update the node part the other property of ExecNodes keeps
             * original
             */
            Assert(streamNode->origin_consumer_nodes == NULL);
            streamNode->origin_consumer_nodes = (ExecNodes*)copyObject(streamNode->consumer_nodes);
            streamNode->origin_consumer_nodes->nodeList = consumer_nodeList;

            streamNode->consumer_nodes->nodeList = cteplan_nodeList;
            producer_topplan->exec_nodes->nodeList = cteplan_nodeList;
            plan->exec_nodes->nodeList = cteplan_nodeList;
        }
    }

    /*
     * In order to print plan when the 'hang' plan occur, we do check here.
     * (1) Check if current consumer nodes match parent producer exec nodes.
     * (2) Check dop either.
     * (3) For local stream node we do not check the exec nodes cause consumer nodes
     * will be modified forcibly or will be marked as dummy thread, so just skipping.
     * (4) For top node of plan tree, whether need to check consumer_nodes when it is stream node,
     * there are two cases:
     *     1)case one: for R8C10 we do not need to check because R8 has modified the judgment
     * method of dummy thread to deal with this kind of 'hang' plan.
     *     2)case two: for R7C10 we still need check to avoid hang up.
     * Finally, report error when they mismatch.
     *
     * In case of startAll, we may make a top-plan node in current producer thread to
     * full nodeList to avoid incorrect reporting 'Stream plan check failed', so just skip it
     */
    bool isTopStreamNode = ((IsA(ctl->pstmt->planTree, Stream) || IsA(ctl->pstmt->planTree, VecStream)) &&
                               ctl->pstmt->planTree->plan_node_id == ctl->plan->plan_node_id)
                                ? true : false;
    if (!isTopStreamNode && !startall) {
        CheckStreamMatchInfo(ctl->checkInfo,
            plan->plan_node_id,
            streamNode->consumer_nodes->nodeList,
            streamNode->smpDesc.consumerDop,
            isLocalStream);
    }

    /* Backup checkinfo (for current stream thread layer) and save checkInfo for the next node */
    List* saved_parentProducerExecNodeList = ctl->checkInfo.parentProducerExecNodeList;
    int saved_parentProducerDop = streamNode->smpDesc.producerDop;
    ctl->checkInfo.parentProducerExecNodeList = plan->exec_nodes->nodeList;
    ctl->checkInfo.parentProducerDop = streamNode->smpDesc.producerDop;

    execNodes = plan->exec_nodes->nodeList;
#ifdef ENABLE_MULTIPLE_NODES
    /*
     * If local stream node, the execnode is the same of current PGXCNode,
     * which is also the same of consumer_nodes. And we need to check if this
     * data node is a real execute node, or we will add local stream on dummy node.
     */
    if (STREAM_IS_LOCAL_NODE(streamNode->smpDesc.distriType) &&
        list_member_int(execNodes, u_sess->pgxc_cxt.PGXCNodeId)) {
        execNodes = lappend_int(NIL, u_sess->pgxc_cxt.PGXCNodeId);
        streamNode->consumer_nodes->nodeList = execNodes;
    }

    Assert(execNodes != NULL);
    Assert(list_length(execNodes) > 0);
    Assert(streamNode->consumer_nodes->nodeList != NULL);
    Assert(list_length(streamNode->consumer_nodes->nodeList) > 0);
    Assert(plan->righttree == NULL);
#endif
    /* 1. Start the setup the Consumer part */
    if (ctl->dummyThread == false) {
        /* Initialize the consumer object. */
        for (i = 0; i < streamNode->smpDesc.consumerDop; i++) {
            consumer = New(u_sess->stream_cxt.stream_runtime_mem_cxt) StreamConsumer(
                u_sess->stream_cxt.stream_runtime_mem_cxt);

            /* Set smp identifier. */
            key.smpIdentifier = i;
            consumer->init(key, execNodes, streamNode->smpDesc, transType, sharedContext);

            consumerSMPList = lappend(consumerSMPList, consumer);
            *(ctl->allConsumerList) = lappend(*(ctl->allConsumerList), consumer);
            *(ctl->subConsumerList) = lappend(*(ctl->subConsumerList), consumer);

            /* store its original producer node list */
            if (startall) {
                consumer->m_originProducerNodeList = producer_nodeList;
            }
        }
    }

    /* 2. Start the setup the Producer part */
    consumerNum = list_length(streamNode->consumer_nodes->nodeList);

    /* Set connection number of producer. */
    if (STREAM_IS_LOCAL_NODE(streamNode->smpDesc.distriType))
        producerConnNum = streamNode->smpDesc.consumerDop;
    else
        producerConnNum = consumerNum * streamNode->smpDesc.consumerDop;

    /*
     * Check with lefttree is OK, because exec_nodes is the same as current Stream node.
     * If lefttree is Steam, exec_nodes of current Stream node is the same as consumer_nodes
     * of Stream node in lefttree.
     * In addition, ThreadIsDummy routine has dealt with the case that lefttree is also a stream node.
     */
    streamNode->is_dummy = ThreadIsDummy(plan->lefttree);

    /* If not dummy, we need to establish the connection. */
    if (streamNode->is_dummy == false) {
        for (i = 0; i < streamNode->smpDesc.producerDop; i++) {
            /* Set smp identifier. */
            key.smpIdentifier = i;
            producer = New(u_sess->stream_cxt.stream_runtime_mem_cxt) StreamProducer(
                key, pstmt, streamNode, u_sess->stream_cxt.stream_runtime_mem_cxt, producerConnNum, transType);
            producer->setSharedContext(sharedContext);
            producer->setUniqueSQLKey(u_sess->unique_sql_cxt.unique_sql_id,
                u_sess->unique_sql_cxt.unique_sql_user_id, u_sess->unique_sql_cxt.unique_sql_cn_id);
            producer->setGlobalSessionId(&u_sess->globalSessionId);
            producerSMPList = lappend(producerSMPList, producer);

            /* Add all producer to node group to avoid possible consumer-not-deinit */
            *(ctl->allProducerList) = lappend(*(ctl->allProducerList), producer);
            *(ctl->subProducerList) = lappend(*(ctl->subProducerList), producer);

            if (startall) {
                /* Store the origin list */
                producer->m_originConsumerNodeList = consumer_nodeList;
                producer->m_originProducerExecNodeList = producer_nodeList;
            }
        }
    } else {
        key.smpIdentifier = 0;
        producer = New(u_sess->stream_cxt.stream_runtime_mem_cxt)
            StreamProducer(key, pstmt, streamNode, u_sess->stream_cxt.stream_runtime_mem_cxt, consumerNum, transType);
        producer->setUniqueSQLKey(u_sess->unique_sql_cxt.unique_sql_id,
            u_sess->unique_sql_cxt.unique_sql_user_id, u_sess->unique_sql_cxt.unique_sql_cn_id);
        producer->setGlobalSessionId(&u_sess->globalSessionId);
        producerSMPList = lappend(producerSMPList, producer);
    }

    StreamPair* pair = u_sess->stream_cxt.global_obj->pushStreamPair(key, producerSMPList, consumerSMPList);

    Assert(pair != NULL);
    List* producerList = NULL;
    List* consumerList = NULL;
    int subInnerThreadNum = 0; /* Number of stream nodes below current stream nodes. */

    /* Traverse left tree. */
    StreamFlowCtl lctl = *ctl;
    lctl.plan = plan->lefttree;
    lctl.dummyThread = streamNode->is_dummy;
    lctl.subProducerList = &producerList;
    lctl.subConsumerList = &consumerList;
    lctl.threadNum = &subInnerThreadNum;
    /* checkInfo should be passed down. */
    InitStreamFlow(&lctl);

    /* Sub plan in the left tree, also the sub plan. */
    ListCell* l1 = NULL;
    ListCell* l2 = NULL;
    forboth(l1, pstmt->subplans, l2, pstmt->subplan_ids)
    {
        if (plan->lefttree->plan_node_id == lfirst_int(l2)) {
            lctl.plan = (Plan*)lfirst(l1);
            /* Set parent info as checkInfo for every init plan. */
            SetCheckInfo(&lctl.checkInfo, plan->lefttree);
            InitStreamFlow(&lctl);
        }
    }

    *(ctl->threadNum) += subInnerThreadNum + streamNode->smpDesc.producerDop;
    pair->expectThreadNum = subInnerThreadNum;

    /* Set some args. */
    ListCell* cell = NULL;
    foreach (cell, producerSMPList) {
        producer = (StreamProducer*)lfirst(cell);

        producer->setPair(pair);
        producer->setSubProducerList(producerList);
        producer->setSubConsumerList(consumerList);
        producer->setSessionMemory(t_thrd.shemem_ptr_cxt.mySessionMemoryEntry);
    }

    *(ctl->subProducerList) = list_union_ptr(*(ctl->subProducerList), producerList);

    /*
     * After init the underlying stream workflow, we need restore parentProducerDop
     * and parentProducerExecNodeList in current stream thread level
     */
    ctl->checkInfo.parentProducerExecNodeList = saved_parentProducerExecNodeList;
    ctl->checkInfo.parentProducerDop = saved_parentProducerDop;
}

/*
 * @Description: Traverse the plan tree to find stream node
 *
 * @param[IN] ctl:  controller used to init stream
 * @return: void
 */
static void InitStreamFlow(StreamFlowCtl* ctl)
{
    if (ctl->plan) {
        Plan* oldPlan = ctl->plan;
        StreamFlowCheckInfo oldCheckInfo = ctl->checkInfo;
        switch (nodeTag(oldPlan)) {
            case T_Append:
            case T_VecAppend: {
                Append* append = (Append*)oldPlan;
                ListCell* lc = NULL;
                foreach (lc, append->appendplans) {
                    Plan* subplan = (Plan*)lfirst(lc);
                    ctl->plan = subplan;
                    /* Set parent info as checkInfo for every sub plan. */
                    SetCheckInfo(&ctl->checkInfo, oldPlan);
                    InitStreamFlow(ctl);
                }
            } break;
            case T_ModifyTable:
            case T_VecModifyTable: {
                ModifyTable* mt = (ModifyTable*)oldPlan;
                ListCell* lc = NULL;
                foreach (lc, mt->plans) {
                    Plan* subplan = (Plan*)lfirst(lc);
                    ctl->plan = subplan;
                    /* Set parent info as checkInfo for every sub plan. */
                    SetCheckInfo(&ctl->checkInfo, oldPlan);
                    InitStreamFlow(ctl);
                }
            } break;
            case T_SubqueryScan:
            case T_VecSubqueryScan: {
                SubqueryScan* ss = (SubqueryScan*)oldPlan;
                if (ss->subplan) {
                    ctl->plan = ss->subplan;
                    /* Set parent info as checkInfo for every sub plan. */
                    SetCheckInfo(&ctl->checkInfo, oldPlan);
                    InitStreamFlow(ctl);
                }
            } break;
            case T_MergeAppend: {
                MergeAppend* ma = (MergeAppend*)oldPlan;
                ListCell* lc = NULL;
                foreach (lc, ma->mergeplans) {
                    Plan* subplan = (Plan*)lfirst(lc);
                    ctl->plan = subplan;
                    /* Set parent info as checkInfo for every sub plan. */
                    SetCheckInfo(&ctl->checkInfo, oldPlan);
                    InitStreamFlow(ctl);
                }
            } break;
            case T_BitmapAnd:
            case T_CStoreIndexAnd: {
                BitmapAnd* ba = (BitmapAnd*)oldPlan;
                ListCell* lc = NULL;
                foreach (lc, ba->bitmapplans) {
                    Plan* subplan = (Plan*)lfirst(lc);
                    ctl->plan = subplan;
                    /* Set parent info as checkInfo for every sub plan. */
                    SetCheckInfo(&ctl->checkInfo, oldPlan);
                    InitStreamFlow(ctl);
                }
            } break;
            case T_BitmapOr:
            case T_CStoreIndexOr: {
                BitmapOr* bo = (BitmapOr*)oldPlan;
                ListCell* lc = NULL;
                foreach (lc, bo->bitmapplans) {
                    Plan* subplan = (Plan*)lfirst(lc);
                    ctl->plan = subplan;
                    /* Set parent info as checkInfo for every sub plan. */
                    SetCheckInfo(&ctl->checkInfo, oldPlan);
                    InitStreamFlow(ctl);
                }
            } break;
            case T_Stream:
            case T_VecStream: {
                InitStream(ctl, STREAM_COMM);

                /* Create stream controller for current stream node */
                if (NeedSetupSyncUpController(oldPlan)) {
                    ExecSyncControllerCreate(oldPlan);
                }
            } break;
            case T_RecursiveUnion: {
                /* Create recursive-controller for current recursive-union node */
                if (NeedSetupSyncUpController(oldPlan)) {
                    ExecSyncControllerCreate(oldPlan);
                }

                ctl->plan = oldPlan->lefttree;
                InitStreamFlow(ctl);

                /* Before traverse the right tree, we must reset the checkInfo. */
                ctl->checkInfo = oldCheckInfo;
                ctl->plan = oldPlan->righttree;
                InitStreamFlow(ctl);
            } break;
            default:
                if (oldPlan->lefttree) {
                    ctl->plan = oldPlan->lefttree;
                    ctl->checkInfo = oldCheckInfo;
                    InitStreamFlow(ctl);
                }
                if (oldPlan->righttree) {
                    ctl->plan = oldPlan->righttree;
                    ctl->checkInfo = oldCheckInfo;
                    InitStreamFlow(ctl);
                }

                break;
        }

        ctl->plan = oldPlan;
    }
}

/*
 * @Description: Destroy streamRuntimeContext and t_thrd.mem_cxt.data_exchange_mem_cxt.
 *
 * @return: void
 */
void DeinitStreamContext()
{
    /* Reset the stream runtime context now for next query. */
    if (u_sess->stream_cxt.stream_runtime_mem_cxt != NULL) {
        MemoryContextDelete(u_sess->stream_cxt.stream_runtime_mem_cxt);
        u_sess->stream_cxt.stream_runtime_mem_cxt = NULL;
    }

    if (IS_PGXC_DATANODE) {
        /* Reset the shared memory context now for next query. */
        if (u_sess->stream_cxt.data_exchange_mem_cxt != NULL) {
            MemoryContextDelete(u_sess->stream_cxt.data_exchange_mem_cxt);
            u_sess->stream_cxt.data_exchange_mem_cxt = NULL;
        }
    }
}

/*
 * @Description: Create streamRuntimeContext and t_thrd.mem_cxt.data_exchange_mem_cxt for stream.
 *
 * @return: void
 */
void InitStreamContext()
{
    char context_name[NAMEDATALEN] = {0};
    errno_t rc = EOK;

    /* Append tid to identify each openGauss thread. */
    rc = snprintf_s(context_name, NAMEDATALEN, NAMEDATALEN - 1, "StreamRuntimeContext__%lu", u_sess->debug_query_id);
    securec_check_ss(rc, "\0", "\0");
    u_sess->stream_cxt.stream_runtime_mem_cxt = AllocSetContextCreate(g_instance.instance_context,
        context_name,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        SHARED_CONTEXT);

    if (IS_PGXC_DATANODE) {
        /* Append tid to identify each openGauss thread. */
        rc = snprintf_s(
            context_name, NAMEDATALEN, NAMEDATALEN - 1, "MemoryDataExchangeContext_%lu", u_sess->debug_query_id);
        securec_check_ss(rc, "\0", "\0");
        u_sess->stream_cxt.data_exchange_mem_cxt = AllocSetContextCreate(g_instance.instance_context,
            context_name,
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);
    }
}

/*
 * @Description: Init stream node group, init all stream objects and complete stream connection
 *
 * @param[IN] plan:  plan stmt
 * @return: void
 */
void BuildStreamFlow(PlannedStmt* plan)
{
    List* consumerList = NIL;
    List* producerList = NIL;
    List* subProducerList = NIL;
    ListCell* producerCell = NULL;
    ListCell* consumerCell = NULL;
    StreamConsumer* consumer = NULL;
    StreamProducer* producer = NULL;

    if (plan->num_streams > 0 && u_sess->stream_cxt.global_obj == NULL) {
        AutoContextSwitch streamCxtGuard(u_sess->stream_cxt.stream_runtime_mem_cxt);
        int threadNum = 0;
        List* topConsumerList = NULL;

        /* Hold interrupts during stream connection and thread initialization. */
        HOLD_INTERRUPTS();

        u_sess->stream_cxt.global_obj = New(u_sess->stream_cxt.stream_runtime_mem_cxt) StreamNodeGroup();
        u_sess->stream_cxt.global_obj->m_streamRuntimeContext = u_sess->stream_cxt.stream_runtime_mem_cxt;

        StreamFlowCtl ctl;
        ctl.pstmt = plan;
        ctl.plan = plan->planTree;
        ctl.allConsumerList = &consumerList;
        ctl.allProducerList = &producerList;
        ctl.subProducerList = &subProducerList;
        ctl.subConsumerList = &topConsumerList;
        ctl.threadNum = &threadNum;
        ctl.dummyThread = ThreadIsDummy(plan->planTree);
        /* Init check info. */
        SetCheckInfo(&ctl.checkInfo, plan->planTree);

        InitStreamFlow(&ctl);

        ListCell* l1 = NULL;
        ListCell* l2 = NULL;
        forboth(l1, plan->subplans, l2, plan->subplan_ids)
        {
            if (plan->planTree->plan_node_id == lfirst_int(l2)) {
                ctl.plan = (Plan*)lfirst(l1);
                /* Set parent info as checkInfo for every init plan. */
                SetCheckInfo(&ctl.checkInfo, plan->planTree);
                InitStreamFlow(&ctl);
            }
        }

        u_sess->stream_cxt.global_obj->m_streamConsumerList = consumerList;
        u_sess->stream_cxt.global_obj->m_streamProducerList = producerList;
        u_sess->stream_cxt.global_obj->Init(threadNum);

        /* Resume interrupts after initialization of u_sess->stream_cxt.global_obj. */
        RESUME_INTERRUPTS();

        if (topConsumerList != NIL) {
            /* Register top consumer thread consumer object. */
            foreach (consumerCell, topConsumerList) {
                consumer = (StreamConsumer*)lfirst(consumerCell);
                u_sess->stream_cxt.global_obj->registerStream(consumer);
            }
        }

        if (producerList != NIL) {
            /* collect connect consumer time */
            if (plan->instrument_option)
                TRACK_START(-1, STREAMNET_INIT);

            int total_connNum = 0;
            int current_index = 0;
            /* Connect related consumer. */
            foreach (producerCell, producerList) {
                producer = (StreamProducer*)lfirst(producerCell);
                /*
                 * Here, total_connNum only counts for non local stream nodes.
                 * Because producer of local stream does not need to connect with consumer.
                 */
                if (!producer->isLocalStream()) {
                    total_connNum += producer->getConnNum();
                } 
            }

            /* We only create connection for non local stream. */
            if (total_connNum > 0) {
                libcomm_addrinfo** total_consumerAddr = 
                    (libcomm_addrinfo**)palloc0(total_connNum * sizeof(libcomm_addrinfo*));
                foreach (producerCell, producerList) {
                    producer = (StreamProducer*)lfirst(producerCell);
                    if (!producer->isLocalStream()) {
                        IPC_PERFORMANCE_LOG_OUTPUT("BuildStreamFlow producer connectConsumer start.");
                        producer->connectConsumer(total_consumerAddr, current_index, total_connNum);
                        IPC_PERFORMANCE_LOG_OUTPUT("BuildStreamFlow producer connectConsumer end.");
                    }
                }

                StreamConnectNodes(total_consumerAddr, total_connNum);
            }
            if (plan->instrument_option)
                TRACK_END(-1, STREAMNET_INIT);
        }

        if (consumerList != NIL) {
            /* Wait for all producer connection ready. */
            foreach (consumerCell, consumerList) {
                consumer = (StreamConsumer*)lfirst(consumerCell);
                IPC_PERFORMANCE_LOG_OUTPUT("BuildStreamFlow waitProducerReady start.");
                consumer->waitProducerReady();
                IPC_PERFORMANCE_LOG_OUTPUT("BuildStreamFlow waitProducerReady end.");
            }
        }
    }
}

void SetupStreamRuntime(StreamState* node)
{
    StreamKey key;
    Stream* streamNode = (Stream*)node->ss.ps.plan;
    StreamPair* pair = NULL;
    StreamConsumer* consumer = NULL;

    key.queryId = node->ss.ps.state->es_plannedstmt->queryId;
    key.planNodeId = streamNode->scan.plan.plan_node_id;

    Assert(u_sess->stream_cxt.global_obj != NULL);
    pair = u_sess->stream_cxt.global_obj->popStreamPair(key);

    Assert(pair->producerList != NULL);

    /* Set the current smp id. */
    key.smpIdentifier = u_sess->stream_cxt.smp_id;

    if (pair->consumerList) {
        consumer = (StreamConsumer*)FindStreamObjBySmpId(pair->consumerList, u_sess->stream_cxt.smp_id);
        Assert(consumer != NULL);
    }

    /* Set shared context in StreamState. */
    if (pair->producerList) {
        StreamProducer* producer = (StreamProducer*)linitial(pair->producerList);
        node->sharedContext = producer->getSharedContext();
    }

    /* Set consumer object in streamState. */
    node->consumer = consumer;

    RegisterStreamSnapshots();
}

static void StartupStreamThread(StreamState* node)
{
    StreamPair* pair = NULL;
    StreamKey key;
    ListCell* cell = NULL;
    uint8 smpId = 0;

    key.queryId = node->ss.ps.state->es_plannedstmt->queryId;
    key.planNodeId = node->ss.ps.plan->plan_node_id;
    Assert(u_sess->stream_cxt.global_obj != NULL);
    pair = u_sess->stream_cxt.global_obj->popStreamPair(key);
    Assert(pair->producerList != NULL);

    StreamTxnContext transactionCxt;
    transactionCxt.txnId = GetCurrentTransactionIdIfAny();
    transactionCxt.snapshot = node->ss.ps.state->es_snapshot;
    StreamSaveTxnContext(&transactionCxt);

    /* Only the Top Consumer thread can spawn multiple thread. */
    foreach (cell, pair->producerList) {
        StreamProducer* producer = (StreamProducer*)lfirst(cell);
        producer->init(node->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor,
                       transactionCxt,
                       node->ss.ps.state->es_param_list_info,
                       u_sess->stream_cxt.producer_obj ? u_sess->stream_cxt.producer_obj->getKey().planNodeId : 0);
        u_sess->stream_cxt.global_obj->initStreamThread(producer, smpId, pair);
        smpId++;
    }
}

/* Set up Stream thread in parallel */
void StartUpStreamInParallel(PlannedStmt* pstmt, EState* estate)
{
    if (!IS_PGXC_DATANODE || pstmt->num_streams <= 0) {
        return;
    }

    if (pstmt->planTree->plan_node_id > 0) {
        TRACK_START(pstmt->planTree->plan_node_id, DN_STREAM_THREAD_INIT);
    }

    Assert(u_sess->stream_cxt.global_obj != NULL);
    const List *pairList = u_sess->stream_cxt.global_obj->getStreamPairList();
    ListCell *lc = NULL;
    foreach(lc, pairList) {
        StreamPair *pair = (StreamPair*)lfirst(lc);
        StreamProducer *producer = (StreamProducer *)linitial(pair->producerList);
        const Stream *node = producer->getStream();
        /* Do not need to start up thread for dummy node. */
        if (node->is_dummy) {
            continue;
        }

        /* create state to set up stream thread. */
        StreamState *stream_state = makeNode(StreamState);
        stream_state->ss.ps.plan = (Plan*)node;
        stream_state->ss.ps.state = estate;

        ExecInitResultTupleSlot(estate, &stream_state->ss.ps);
        if (node->scan.plan.targetlist) {
            TupleDesc typeInfo = ExecTypeFromTL(node->scan.plan.targetlist, false);
            ExecSetSlotDescriptor(stream_state->ss.ps.ps_ResultTupleSlot, typeInfo);
        } else {
            /* In case there is no target list, force its creation */
            ExecAssignResultTypeFromTL(&stream_state->ss.ps);
        }

        StartupStreamThread(stream_state);

        pfree(stream_state);
    }

    if (pstmt->planTree->plan_node_id > 0) {
        TRACK_END(pstmt->planTree->plan_node_id, DN_STREAM_THREAD_INIT);
    }
}

void StreamPrepareRequestForRecursive(StreamState* node, bool stepsync)
{
    if (!EXEC_IN_RECURSIVE_MODE(node->ss.ps.plan)) {
        elog(ERROR, "MPP with-recursive invalid stream node status Stream[%d]", node->ss.ps.plan->plan_node_id);
    }

    int i = 0;
    int producerNum = 0;
    StreamTransport** transport = node->consumer->getTransport();
    node->need_fresh_data = true;
    node->last_conn_idx = 0;
    StreamCOMM* scomm = NULL;
    bool skip_pruned_datanode = false;
    int full_conn_count = node->consumer->getConnNum();
    int full_conn_index = 0;

    if (stepsync) {
        /*
         * In syncup case, we need receive all dn's message in recursive
         * union execnode scope.
         */
        producerNum = node->consumer->getConnNum();
    } else {
        /*
         * In normal case, we need check if the backup producer-list is set, "not-NIL"
         * indicates that there is a DN-pruning case happen where all connections is
         * setup for plan-step syncup, we need logically pick up the producer connection
         * do correct BROADCAST/REDISTRIBUTE
         */
        if (node->consumer->m_originProducerNodeList != NIL) {
            producerNum = list_length(node->consumer->m_originProducerNodeList);
            skip_pruned_datanode = true;
        } else {
            producerNum = node->consumer->getConnNum();
        }
    }

    /* We should have at least one producer to receive */
    Assert(producerNum > 0);

    node->connections = (PGXCNodeHandle**)palloc0(producerNum * sizeof(PGXCNodeHandle*));
    node->conn_count = producerNum;

    node->netctl.layer.sctpLayer.datamarks = (int*)palloc0(producerNum * sizeof(int));
    node->netctl.layer.sctpLayer.gs_sock = (gsocket*)palloc0(producerNum * sizeof(gsocket));
    node->netctl.layer.sctpLayer.poll2conn = (int*)palloc0(producerNum * sizeof(int));

    /* Loop the full datanode list and skip some of datanode when we found it is pruned */
    for (full_conn_index = 0; full_conn_index < full_conn_count; full_conn_index++) {
        /* skip those do not exists in original consumer NodeList */
        if (skip_pruned_datanode &&
            !list_member_int(node->consumer->m_originProducerNodeList,
            node->consumer->getNodeIdx(transport[full_conn_index]->m_nodeName))) {
            continue;
        }

        scomm = (StreamCOMM*)transport[full_conn_index];

        node->connections[i] = (PGXCNodeHandle*)palloc0(sizeof(PGXCNodeHandle));
        /* Initialise Input buffer */
        node->connections[i]->inSize = STREAM_BUFFER_SIZE;
        node->connections[i]->inBuffer = (char*)palloc0(STREAM_BUFFER_SIZE);
        node->connections[i]->sock = NO_SOCKET;
        node->connections[i]->state = DN_CONNECTION_STATE_QUERY;
        node->connections[i]->stream = node;
        node->connections[i]->remoteNodeName = &transport[full_conn_index]->m_nodeName[0];
        node->connections[i]->nodeIdx = node->consumer->getNodeIdx(transport[full_conn_index]->m_nodeName);
        node->connections[i]->nodeoid = transport[full_conn_index]->m_nodeoid;
        node->connections[i]->gsock = scomm->m_addr->gs_sock;
        node->connections[i]->tcpCtlPort = -1;
        node->connections[i]->listenPort = scomm->m_addr->listen_port;

        /* Check if actual index exceeds the producer number */
        if (i >= producerNum) {
            elog(ERROR, "MPP with-recursive invalid connection index in DN pruning scenarios");
        }

        i++;
    }

    if (node->ss.ps.instrument && node->type == STREAM_BROADCAST)
        node->spill_size = &node->ss.ps.instrument->sorthashinfo.spill_size;
    else
        node->spill_size = &node->recvDataLen;

    node->isReady = true;
}

void StreamPrepareRequest(StreamState* node)
{
    /*
     * If we found current stream node is under recursive union node, we handle the
     * stream preparation in a special way
     *
     * MPP support with-recursive
     */
    if (EXEC_IN_RECURSIVE_MODE(node->ss.ps.plan)) {
        StreamPrepareRequestForRecursive(node, false);

        return;
    }

    int i = 0;
    int producerNum = node->consumer->getConnNum();
    StreamTransport** transport = node->consumer->getTransport();

    node->need_fresh_data = true;
    node->last_conn_idx = 0;

    StreamCOMM* scomm = NULL;

    node->connections = (PGXCNodeHandle**)palloc0(producerNum * sizeof(PGXCNodeHandle*));
    node->conn_count = producerNum;

    node->netctl.layer.sctpLayer.datamarks = (int*)palloc0(producerNum * sizeof(int));
    node->netctl.layer.sctpLayer.gs_sock = (gsocket*)palloc0(producerNum * sizeof(gsocket));
    node->netctl.layer.sctpLayer.poll2conn = (int*)palloc0(producerNum * sizeof(int));

    for (i = 0; i < node->conn_count; i++) {
        scomm = (StreamCOMM*)transport[i];

        node->connections[i] = (PGXCNodeHandle*)palloc0(sizeof(PGXCNodeHandle));
        /* Initialise Input buffer */
        node->connections[i]->inSize = STREAM_BUFFER_SIZE;
        node->connections[i]->inBuffer = (char*)palloc0(node->connections[i]->inSize);
        node->connections[i]->sock = NO_SOCKET;
        node->connections[i]->state = DN_CONNECTION_STATE_QUERY;
        node->connections[i]->stream = node;
        node->connections[i]->remoteNodeName = &transport[i]->m_nodeName[0];
        node->connections[i]->nodeIdx = node->consumer->getNodeIdx(transport[i]->m_nodeName);
        node->connections[i]->nodeoid = transport[i]->m_nodeoid;
        node->connections[i]->gsock = scomm->m_addr->gs_sock;
        node->connections[i]->tcpCtlPort = -1;
        node->connections[i]->listenPort = scomm->m_addr->listen_port;
    }

    if (node->ss.ps.instrument && node->type == STREAM_BROADCAST)
        node->spill_size = &node->ss.ps.instrument->sorthashinfo.spill_size;
    else
        node->spill_size = &node->recvDataLen;

    node->isReady = true;
}

void StreamReportError(StreamState* node)
{
    if (node->errorMessage) {
        if (node->errorDetail != NULL && node->errorContext != NULL)
            ereport(ERROR,
                (errcode(node->errorCode),
                    combiner_errdata(&node->remoteErrData),
                    errmsg("%s", node->errorMessage),
                    errdetail("%s", node->errorDetail),
                    errcontext("%s", node->errorContext)));
        else if (node->errorDetail != NULL)
            ereport(ERROR,
                (errcode(node->errorCode),
                    combiner_errdata(&node->remoteErrData),
                    errmsg("%s", node->errorMessage),
                    errdetail("%s", node->errorDetail)));
        else if (node->errorContext != NULL)
            ereport(ERROR,
                (errcode(node->errorCode),
                    combiner_errdata(&node->remoteErrData),
                    errmsg("%s", node->errorMessage),
                    errcontext("%s", node->errorContext)));
        else
            ereport(ERROR,
                (errcode(node->errorCode), combiner_errdata(&node->remoteErrData), errmsg("%s", node->errorMessage)));
    }
}

void AddCheckMessage(StringInfo msg_new, StringInfo msg_org, bool is_stream, unsigned int planNodeId)
{
    /* Set cursor */
    msg_new->cursor = msg_org->cursor;

    if (is_stream) {
        /* Add queryId check. */
        pq_sendint64(msg_new, u_sess->debug_query_id);
        /* Add planNodeId check. */
        pq_sendint(msg_new, planNodeId, 4);
    }

    /* Add actual message length. */
    pq_sendint(msg_new, msg_org->len, 4);

    /* Add CRC check. */
    pg_crc32 valcrc;
    INIT_CRC32(valcrc);
    COMP_CRC32(valcrc, msg_org->data, msg_org->len);
    FIN_CRC32(valcrc);
    pq_sendint(msg_new, valcrc, 4);

    /* Add actual message. */
    appendBinaryStringInfo(msg_new, msg_org->data, msg_org->len);
}

void CheckMessages(uint64 check_query_id, uint32 check_plan_node_id, char* msg, int msg_len, bool is_stream)
{
    uint32 plan_node_id;
    uint64 query_id;
    int actual_msg_len = 0;
    int head_len = is_stream ? STREAM_CHECKMSG_LEN : REMOTE_CHECKMSG_LEN;
    if (msg_len <= head_len) {
        ereport(ERROR,
            (errmodule(MOD_GUC),
                errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
                errmsg(
                    "message length %d is less than head length  %d", msg_len, head_len)));
    }
    uint32 send_check_sum;
    errno_t rc = EOK;

    if (is_stream) {
        /* Debug query ID check. */
        rc = memcpy_s(&query_id, sizeof(uint64), msg, sizeof(uint64));
        securec_check(rc, "\0", "\0");
        query_id = ntohl64(query_id);
        msg += 8;

        Assert(query_id == check_query_id);
        if (query_id != (uint64)check_query_id) {
            ereport(ERROR,
                (errmodule(MOD_GUC),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("expected query id is %lu, actual query id is %lu", u_sess->debug_query_id, query_id)));
        }

        /* Plan Node ID check. */
        rc = memcpy_s(&plan_node_id, sizeof(int), msg, sizeof(int));
        securec_check(rc, "\0", "\0");
        plan_node_id = ntohl(plan_node_id);
        msg += 4;

        Assert(check_plan_node_id == plan_node_id);

        if (check_plan_node_id != plan_node_id) {
            ereport(ERROR,
                (errmodule(MOD_GUC),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg(
                        "expected plan node id is %u, actual plan node id is %u", check_plan_node_id, plan_node_id)));
        }
    }

    /* Message length check. */
    rc = memcpy_s(&actual_msg_len, sizeof(int), msg, sizeof(int));
    securec_check(rc, "\0", "\0");
    actual_msg_len = ntohl(actual_msg_len);
    msg += 4;
    Assert(actual_msg_len == msg_len - head_len);

    if (actual_msg_len != msg_len - head_len) {
        ereport(ERROR,
            (errmodule(MOD_GUC),
                errcode(ERRCODE_STRING_DATA_LENGTH_MISMATCH),
                errmsg(
                    "expected message length is %d, actual message length is %d", actual_msg_len, msg_len - head_len)));
    }

    /* CRC check. */
    rc = memcpy_s(&send_check_sum, sizeof(uint32), msg, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    send_check_sum = ntohl(send_check_sum);
    msg += 4;

    pg_crc32 valcrc;
    INIT_CRC32(valcrc);
    COMP_CRC32(valcrc, msg, actual_msg_len);
    FIN_CRC32(valcrc);

    Assert(send_check_sum == valcrc);

    if (send_check_sum != valcrc) {
        ereport(ERROR,
            (errmodule(MOD_GUC),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("expected crc is %u, actual crc is %u", send_check_sum, valcrc)));
    }
}
static void HandleStreamTuple(StreamState* node, char* msg, int msg_len)
{
    errno_t rc = EOK;

    if (node->errorMessage)
        return;

    /* We expect previous message is consumed */
    if (!EXEC_IN_RECURSIVE_MODE(node->ss.ps.plan)) {
        /*
         * If stream is mark as no early free (e.g. recursive-union case), we need it
         * to response for sync cluster step, we don't do assert here.
         */
        AssertEreport(node->buf.len == 0, MOD_OPT, "node buf len is not zero");
    }

    /* Check messages. */
#ifdef USE_ASSERT_CHECKING
    CheckMessages(node->consumer->getKey().queryId, ((PlanState*)node)->plan->plan_node_id, msg, msg_len, true);
    msg += STREAM_CHECKMSG_LEN;
    msg_len -= STREAM_CHECKMSG_LEN;
#else

    if (unlikely(anls_opt_is_on(ANLS_STREAM_DATA_CHECK))) {
        CheckMessages(node->consumer->getKey().queryId, ((PlanState*)node)->plan->plan_node_id, msg, msg_len, true);
        msg += STREAM_CHECKMSG_LEN;
        msg_len -= STREAM_CHECKMSG_LEN;
    }
#endif

    if (msg_len < 0)
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Unexpected response from remote node")));

    if (msg_len > node->buf.size) {
        node->buf.msg = (char*)repalloc(node->buf.msg, msg_len);
        node->buf.size = msg_len;
    }

    rc = memcpy_s(node->buf.msg, node->buf.size, msg, msg_len);
    securec_check(rc, "\0", "\0");
    node->buf.len = msg_len;

    /* Record length of all received data for network perf data. */
    *node->spill_size += msg_len;
}

void HandleStreamError(StreamState* node, char* msg_body, int len)
{
    /* parse error message */
    char* code = NULL;
    char* message = NULL;
    char* detail = NULL;
    char* context = NULL;
    size_t offset = 0;
    char* realerrcode = NULL;
    char* funcname = NULL;
    char* filename = NULL;
    char* lineno = NULL;
    int error_code = 0;
    char* mod_id = NULL;

    /*
     * Scan until point to terminating \0
     */
    while (((int)(offset + 1)) < len) {
        /* pointer to the field message */
        char* str = msg_body + offset + 1;

        switch (msg_body[offset]) {
            case 'c':
                realerrcode = str;
                break;
            case 'C': /* code */
                code = str;

                /* Error Code is exactly 5 significant bytes */
                if (code != NULL)
                    error_code = MAKE_SQLSTATE(code[0], code[1], code[2], code[3], code[4]);
                break;
            case 'M': /* message */
                message = str;
                break;
            case 'D': /* details */
                detail = str;
                break;
            case 'd': /* mod_id */
                mod_id = str;
                break;
            case 'W': /* where */
                context = str;
                break;
            case 'F': /* file */
                filename = str;
                break;
            case 'L': /* line */
                lineno = str;
                break;
            case 'R': /* routine */
                funcname = str;
                break;

            /* Fields not yet in use */
            case 'S': /* severity */
            case 'H': /* hint */
            case 'P': /* position string */
            case 'p': /* position int */
            case 'q': /* int query */
            default:
                break;
        }

        /* code, message and \0 */
        offset += strlen(str) + 2;
    }

    /*
     * We may have special handling for some errors, default handling is to
     * throw out error with the same message. We can not ereport immediately
     * because we should read from this and other connections until
     * ReadyForQuery is received, so we just store the error message.
     * If multiple connections return errors only first one is reported.
     */
    if (node->errorMessage == NULL) {
        MemoryContext old_cxt = MemoryContextSwitchTo(ErrorContext);

        if (message != NULL) {
            node->errorMessage = pstrdup(message);

            if (code != NULL)
                node->errorCode = error_code;

            if (realerrcode != NULL)
                node->remoteErrData.internalerrcode = pg_strtoint32(realerrcode);
        }

        if (detail != NULL)
            node->errorDetail = pstrdup(detail);
        else
            node->errorDetail = NULL;

        if (context != NULL)
            node->errorContext = pstrdup(context);
        else
            node->errorContext = NULL;

        if (filename != NULL)
            node->remoteErrData.filename = pstrdup(filename);
        else
            node->remoteErrData.filename = NULL;

        if (funcname != NULL)
            node->remoteErrData.errorfuncname = pstrdup(funcname);
        else
            node->remoteErrData.errorfuncname = NULL;

        if (lineno != NULL)
            node->remoteErrData.lineno = pg_strtoint32(lineno);
        else
            node->remoteErrData.lineno = 0;

        if (mod_id != NULL)
            node->remoteErrData.mod_id = get_module_id(mod_id);

        MemoryContextSwitchTo(old_cxt);
    }
}

/*
 * Handle NoticeResponse ('N') message from Stream thread
 */
void HandleStreamNotice(StreamState* node, char* msg_body, size_t len)
{
    /* parse error message */
    char* message = NULL;
    char* detail = NULL;
    size_t offset = 0;

    /*
     * Scan until point to terminating \0
     */
    while (offset + 1 < len) {
        /* pointer to the field message */
        char* str = msg_body + offset + 1;

        switch (msg_body[offset]) {
            case 'M': /* message */
                message = str;
                break;
            case 'D': /* details */
                detail = str;
                break;
            /* Fields not yet in use */
            case 'S': /* severity */
            case 'C': /* code */
            case 'R': /* routine */
            case 'H': /* hint */
            case 'P': /* position string */
            case 'p': /* position int */
            case 'q': /* int query */
            case 'W': /* where */
            case 'F': /* file */
            case 'L': /* line */
            default:
                break;
        }

        /* code, message and \0 */
        offset += strlen(str) + 2;
    }

    if (message != NULL) {
        if (detail != NULL)
            ereport(NOTICE, (errmsg("%s", message), errdetail("%s", detail), handle_in_client(true)));
        else
            ereport(NOTICE, (errmsg("%s", message), handle_in_client(true)));
    }
}

/*
 * MPP Recursive-Union support
 */
static void ParseRUSyncMsg(const char* msg, const char* key, char* value)
{
    Assert(msg != NULL && key != NULL && value != NULL);

    const char* value_startptr = (char*)strstr(msg, key) + strlen(key);
    const char* value_endptr = strstr(value_startptr, ",");
    int len = value_endptr - value_startptr;

    errno_t rc = memset_s(value, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check(rc, "\0", "\0");

    rc = memcpy_s(value, NAMEDATALEN, value_startptr, len);
    securec_check(rc, "\0", "\0");
}

static void HandleStreamRUSyncMsg(StreamState* node, char msg_type, char* msg, int msg_len)
{
    /*
     * With-Recursive
     *     nodename:datanode1,
     *     rustep:2,
     *     iteration:1,  <<<<<< only for step2
     *     xcnodeid:0,
     *     tuple_count:0,
     */
    if (!IS_PGXC_DATANODE) {
        return;
    }

    Assert(u_sess->stream_cxt.global_obj != NULL && msg != NULL && msg_len > 0);

    char value[NAMEDATALEN] = {0};

    /* parse node name */
    ParseRUSyncMsg(msg, "nodename:", value);
    char* nodename = pstrdup(value);

    /* parse recursive step id */
    ParseRUSyncMsg(msg, "rustep:", value);
    int rustep = atoi(value);

    /* parse pgxcnodeid */
    ParseRUSyncMsg(msg, "xcnodeid:", value);
    int xcnodeid = atoi(value);

    /* parse iteration */
    ParseRUSyncMsg(msg, "iteration:", value);
    int iteration = atoi(value);

    /* parse pgxcnodeid */
    ParseRUSyncMsg(msg, "tuple_processed:", value);
    int tuple_processed = atoi(value);

    /* parse controller_plannodeid */
    ParseRUSyncMsg(msg, "controller_plannodeid:", value);
    int controller_plannodeid = atoi(value);

    /* parse producer_plannodeid */
    ParseRUSyncMsg(msg, "producer_plannodeid:", value);
    int producer_plannodeid = atoi(value);

    /* output important message */
    const char* direction = NULL;
    if (msg_type == 'F') {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("Unexpected msg type[%d]", msg_type)));
        direction = "[C->N]";
    } else if (msg_type == 'R') {
        direction = "[N->C]";
    } else {
        elog(ERROR, "Unrecogonized message type: '%c' msg:%s", msg_type, msg);
    }

    elog(DEBUG1,
        "MPP with-recursive step%d (C) %s receive step-sync message '%c' "
        "{nodename:%s, rustep:%d, iteration:%d, xcnodeid:%d, controller_plannodeid:%d, producer_plannodeid:%d, "
        "tuple_processed:%d}",
        rustep,
        direction,
        msg_type,
        nodename,
        rustep,
        iteration,
        xcnodeid,
        controller_plannodeid,
        producer_plannodeid,
        tuple_processed);

    /* Mark the received datanode's xx step has finished with xx tuple processed */
    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
    SyncController* controller = stream_nodegroup->GetSyncController(controller_plannodeid);

    /*
     * Report Error when the controller for its belonging RecursiveUnion node is
     * not found.
     */
    if (controller == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg(
                    "MPP With-Recursive sync controller for RecursiveUnion[%d] is not found", controller_plannodeid)));
    }

    stream_nodegroup->ConsumerMarkRUStepStatus(
        (RecursiveUnionController*)controller, rustep, iteration, node, xcnodeid, producer_plannodeid, tuple_processed);

    pfree_ext(nodename);
}

/*
 * Read next message from the connection and update the combiner accordingly
 * If we are in an error state we just consume the messages, and do not proxy
 * Long term, we should look into cancelling executing statements
 * and closing the connections.
 * Return values:
 * RESPONSE_EOF - need to receive more data for the connection
 * RESPONSE_COMPLETE - done with the connection
 * RESPONSE_DATAROW - got data row
 */
int HandleStreamResponse(PGXCNodeHandle* conn, StreamState* node)
{
    char* msg = NULL;
    int msg_len;
    char msg_type;

    for (;;) {
        /* hack now will improve later */
        if (u_sess->stream_cxt.global_obj->m_syncControllers && conn->state == DN_CONNECTION_STATE_IDLE) {
            /* with-recursive case */
            conn->state = DN_CONNECTION_STATE_QUERY;
        } else {
            /* normal case */
            if (conn->state == DN_CONNECTION_STATE_IDLE) {
                ereport(ERROR, (errcode(ERRCODE_INVALID_OPTION),
                         errmsg("state is DN_CONNECTION_STATE_IDLE: %d", DN_CONNECTION_STATE_IDLE)));
            }
        }

        /*
         * If we are in the process of shutting down, we
         * may be rolling back, and the buffer may contain other messages.
         * We want to avoid a procarray exception
         * as well as an error stack overflow.
         */
        if (t_thrd.proc_cxt.proc_exit_inprogress) {
            conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
            elog(WARNING, "DN_CONNECTION_STATE_ERROR_FATAL2 is set when proc_exit_inprogress");
        }

        /* don't read from from the connection if there is a fatal error */
        if (conn->state == DN_CONNECTION_STATE_ERROR_FATAL) {
            elog(WARNING, "HandleStreamResponse returned with DN_CONNECTION_STATE_ERROR_FATAL");
            return RESPONSE_COMPLETE;
        }

        /* No data available, exit */
        if (!HAS_MESSAGE_BUFFERED(conn)) {
            return RESPONSE_EOF;
        }

#ifdef MEMORY_CONTEXT_CHECKING
        size_t old_size = conn->inSize;
#endif

        msg_type = get_message(conn, &msg_len, &msg);

#ifdef MEMORY_CONTEXT_CHECKING
        /* Check all memory contexts when repalloc the memory of buffer */
        if (conn->inSize > old_size) {
            MemoryContextCheck(t_thrd.top_mem_cxt, false);
        }
#endif

        switch (msg_type) {
            case '\0': /* Not enough data in the buffer */
                return RESPONSE_EOF;
                break;
            case 'T': /* RowDescription */
                /* Stream thread should not send row desrciption. */
                ereport(ERROR,
                    (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("Stream thread should not send row desrciption.")));
                break;
            case 'B': /* VectorBatch */
            case 'D': /* DataRow */
                HandleStreamTuple(node, msg, msg_len);
                return RESPONSE_DATAROW;
            case 'E': /* ErrorResponse */
                HandleStreamError(node, msg, msg_len);
                break;
            case 'N': /* NoticeResponse */
                HandleStreamNotice(node, msg, msg_len);
                break;
            case 'Z': { /* End of Stream */
                conn->state = DN_CONNECTION_STATE_IDLE;
                return RESPONSE_COMPLETE;
            }
            case 'R': { /* distributed recursive union support */
                /* Handle Recursive Union sync-up message */
                HandleStreamRUSyncMsg(node, 'R', msg, msg_len);
                return RESPONSE_RECURSIVE_SYNC_R;
            }
            default:
                /* sync lost? */
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OPTION),
                         errmsg("Received unsupported message type: %c", msg_type)));
                conn->state = DN_CONNECTION_STATE_ERROR_FATAL;
                /* stop reading */
                return RESPONSE_COMPLETE;
        }
    }
    /* never happen, but keep compiler quiet */
    return RESPONSE_EOF;
}

void AssembleDataRow(StreamState* node)
{
    MemoryContext oldcontext;
    TupleTableSlot* slot = node->ss.ps.ps_ResultTupleSlot;

    NetWorkTimeDeserializeStart(t_thrd.pgxc_cxt.GlobalNetInstr);

    oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);
    ExecStoreDataRowTuple(node->buf.msg, node->buf.len, InvalidOid, slot, false);
    /* The data has been consumed. */
    node->buf.len = 0;
    MemoryContextSwitchTo(oldcontext);

    NetWorkTimeDeserializeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
}

/*
 * Notice:
 * the function of stream for merge sort is 'getlen_stream',
 * please both modify if need.
 */
static bool GetTupleFromConnBuffer(StreamState* node)
{
    int connIdx = 0;
    PGXCNodeHandle** connections = NULL;

    connIdx = node->last_conn_idx;
    connections = node->connections;

    /* Handle data from all connection. */
    while (connIdx < node->conn_count) {
        int res = HandleStreamResponse(connections[connIdx], node);
        switch (res) {
            /* Try next run. */
            case RESPONSE_EOF: {
                connIdx++;
            } break;
            /* Finish one connection. */
            case RESPONSE_COMPLETE: {
                node->conn_count = node->conn_count - 1;

                /* All finished. */
                if (node->conn_count == 0) {
                    node->need_fresh_data = false;
                    return false;
                }

                if (connIdx < node->conn_count) {
                    connections[connIdx] = connections[node->conn_count];
                }
            } break;
            case RESPONSE_DATAROW: {
                /* If we have message in the buffer, consume it */
                if (node->buf.len != 0) {
                    AssembleDataRow(node);
                    node->need_fresh_data = false;
                    node->last_conn_idx = connIdx;
                    return true;
                }
            } break;
            case RESPONSE_RECURSIVE_SYNC_F:
            case RESPONSE_RECURSIVE_SYNC_R:
                continue;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_FETCH_DATA_FAILED),
                        errmsg("Unexpected response %d from Datanode when get tuple from cnnection buffer.The "
                               "connection idx is %d and the count of active connections is %d.",
                            res,
                            connIdx,
                            node->conn_count)));
                break;
        }
    }

    Assert(connIdx == node->conn_count);
    node->need_fresh_data = true;
    node->last_conn_idx = 0;

    return false;
}

/*
 * @Description: scan stream for logic connection
 *
 * @param[IN] node:  run-time state of stream node
 * @return: bool, true if has data
 */
bool ScanStreamByLibcomm(StreamState* node)
{
    while (node->conn_count) {
        if (node->need_fresh_data) {
            if (datanode_receive_from_logic_conn(node->conn_count, node->connections, &node->netctl, -1)) {
                int error_code = getStreamSocketError(gs_comm_strerror());
                ereport(ERROR,
                    (errcode(error_code),
                        errmsg("Failed to read response from Datanodes. Detail: %s\n", gs_comm_strerror())));
            }
        }

        if (node->StreamDeserialize(node))
            return true;
        else {
            if (node->need_fresh_data == false)
                return false;
        }

        StreamReportError(node);
    }

    return false;
}

bool ScanMemoryStream(StreamState* node)
{
    Assert(((Stream*)(node->ss.ps.plan))->sort == NULL);
    while (node->conn_count) {
        if (node->need_fresh_data) {
            if (gs_memory_recv(node)) {
                return true;
            } else {
                StreamReportError(node);
                node->need_fresh_data = false;
                return false;
            }
        }
    }
    return false;
}

/*
 * Creates the run-time information for the stream broadcast node and redistribute node
 */
StreamState* BuildStreamRuntime(Stream* node, EState* estate, int eflags)
{
    StreamState* stream_state = NULL;

    /* StreamState is allocated in StreamRunTime Memory Context if we need set up controller */
    if (NeedSetupSyncUpController((Plan*)node)) {
        MemoryContext stream_runtime_memctx = u_sess->stream_cxt.global_obj->m_streamRuntimeContext;
        Assert(stream_runtime_memctx != NULL);
        MemoryContext recursive_runtime_memctx = AllocSetContextCreate(stream_runtime_memctx,
            "RecursiveRuntimeContext",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            SHARED_CONTEXT);

        MemoryContext current_memctx = MemoryContextSwitchTo(recursive_runtime_memctx);
        stream_state = makeNode(StreamState);

        /* Swith back to current memory context */
        MemoryContextSwitchTo(current_memctx);
    } else {
        stream_state = makeNode(StreamState);
    }

    stream_state->ss.ps.plan = (Plan*)node;
    stream_state->ss.ps.state = estate;

    ExecInitResultTupleSlot(estate, &stream_state->ss.ps);
    if (node->scan.plan.targetlist) {
        TupleDesc typeInfo = ExecTypeFromTL(node->scan.plan.targetlist, false);
        ExecSetSlotDescriptor(stream_state->ss.ps.ps_ResultTupleSlot, typeInfo);
    } else {
        /* In case there is no target list, force its creation */
        ExecAssignResultTypeFromTL(&stream_state->ss.ps);
    }

    /* Set up underlying execution nodes on coordinator nodes */
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
#else
    if (StreamTopConsumerAmI()) {
#endif
        if (innerPlan(node))
            innerPlanState(stream_state) = ExecInitNode(innerPlan(node), estate, eflags);

        if (outerPlan(node))
            outerPlanState(stream_state) = ExecInitNode(outerPlan(node), estate, eflags);
    } else {
        /* Right tree should be null. */
        Assert(innerPlan(node) == NULL);
    }

    /* Stream runtime only set up on datanode. */
    if (IS_PGXC_DATANODE)
        SetupStreamRuntime(stream_state);

    return stream_state;
}

/*
 * Creates the run-time information for the stream node
 */
StreamState* ExecInitStream(Stream* node, EState* estate, int eflags)
{
    StreamState* state = NULL;

    state = BuildStreamRuntime(node, estate, eflags);

    state->buf.msg = (char*)palloc(STREAM_BUF_INIT_SIZE);
    state->buf.len = 0;
    state->buf.size = STREAM_BUF_INIT_SIZE;
    state->isReady = false;
    state->vector_output = false;
    state->StreamScan = ScanStreamByLibcomm;

    if (STREAM_IS_LOCAL_NODE(node->smpDesc.distriType)) {
        state->StreamScan = ScanMemoryStream;
        TupleDesc typeInfo = ExecTypeFromTL(node->scan.plan.targetlist, false);

        state->tempTupleVec = (TupleVector*)palloc0(sizeof(TupleVector));
        state->tempTupleVec->tupleVector = (TupleTableSlot**)palloc0(sizeof(TupleTableSlot*) * (TupleVectorMaxSize));

        for (int j = 0; j < TupleVectorMaxSize; j++) {
            state->tempTupleVec->tupleVector[j] = MakeTupleTableSlot(false);
            ExecSetSlotDescriptor(state->tempTupleVec->tupleVector[j], typeInfo);
        }
    }

    state->StreamDeserialize = GetTupleFromConnBuffer;
    state->recvDataLen = 0;
    state->type = node->type;
    state->receive_message = false;

    /* create stream controller */
    if (NeedSetupSyncUpController(state->ss.ps.plan)) {
        StreamController* controller =
            (StreamController*)u_sess->stream_cxt.global_obj->GetSyncController(node->scan.plan.plan_node_id);
        if (controller == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                    errmsg("MPP With-Recursive sync controller for Stream[%d] is not found",
                        node->scan.plan.plan_node_id)));
        }
        Assert(controller->controller.controller_type == T_Stream);
        controller->controller.controller_planstate = (PlanState*)state;
    }

    return state;
}

TupleTableSlot* ExecStream(StreamState* node)
{
    if (unlikely(node->isReady == false)) {
        StreamPrepareRequest(node);

        /* merge sort */
        if (((Stream*)(node->ss.ps.plan))->sort != NULL) {
            InitStreamMergeSort(node);
        }
    }

    node->receive_message = true;

    if (node->StreamScan(node)) {
        return node->ss.ps.ps_ResultTupleSlot;
    } else {
        /* Finish receiving data from producers, can set network perf data now. */
        if (HAS_INSTR(&node->ss, false)) {
            u_sess->instr_cxt.global_instr->SetNetWork(node->ss.ps.plan->plan_node_id, *node->spill_size);
        }

        return NULL;
    }
}

/*
 * @Description: If get 0 row in one side of join operator, join may return NULL but stream
 * in the other side still waiting to send data and hold the channel. So we should deinit the
 * consumer earlier.
 *
 * @param[IN] node:  PlanState tree paralleling the Plan tree.
 * @return: void
 */
void ExecEarlyDeinitConsumer(PlanState* node)
{
    /* A Coordinator has no stream thread, so do not bother about that */
    if (IS_PGXC_COORDINATOR)
        return;

    /* Exit if skip early deinit consumer */
    if (node->state->es_skip_early_deinit_consumer)
        return;

    switch (nodeTag(node)) {
        case T_StreamState:
        case T_VecStreamState: {
            /*
             * When the consumer is responsible for sync cluster steps we do not do
             * real consumer-deInit, just consumer its data length
             */
            if (EXEC_IN_RECURSIVE_MODE(node->plan)) {
                ((StreamState*)node)->buf.len = 0;

                /* When the StreamState node under recursive union do not execute, we should receive 'Z' message
                    in case the connections is blocking */
                if (!((StreamState*)node)->receive_message) {
                    StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
                    stream_nodegroup->ConsumerNodeStreamMessage((StreamState*)node);

                    ((StreamState*)node)->isReady = false;
                    ((StreamState*)node)->receive_message = true;
                }
            } else {
                ((StreamState*)node)->consumer->deInit();
            }

            return;
        }
        case T_AppendState:
        case T_VecAppendState: {
            AppendState* appendState = (AppendState*)node;
            for (int planNo = 0; planNo < appendState->as_nplans; planNo++) {
                ExecEarlyDeinitConsumer(appendState->appendplans[planNo]);
            }
        } break;
        case T_ModifyTableState:
        case T_VecModifyTableState:
        case T_DistInsertSelectState: {
            ModifyTableState* mt = (ModifyTableState*)node;
            for (int planNo = 0; planNo < mt->mt_nplans; planNo++) {
                ExecEarlyDeinitConsumer(mt->mt_plans[planNo]);
            }
        } break;
        case T_SubqueryScanState:
        case T_VecSubqueryScanState: {
            SubqueryScanState* ss = (SubqueryScanState*)node;
            if (ss->subplan)
                ExecEarlyDeinitConsumer(ss->subplan);
        } break;
        case T_MergeAppendState: {
            MergeAppendState* ma = (MergeAppendState*)node;
            for (int planNo = 0; planNo < ma->ms_nplans; planNo++) {
                ExecEarlyDeinitConsumer(ma->mergeplans[planNo]);
            }
        } break;
        default:
            if (outerPlanState(node)) {
                ExecEarlyDeinitConsumer(outerPlanState(node));
            }

            if (innerPlanState(node)) {
                ExecEarlyDeinitConsumer(innerPlanState(node));
            }
            break;
    }
}

void ExecEndStream(StreamState* node)
{
    if (IS_PGXC_DATANODE && node->consumer)
        node->consumer->deInit();
#ifndef ENABLE_MULTIPLE_NODES
    if (u_sess->stream_cxt.global_obj) {
        u_sess->stream_cxt.global_obj->SigStreamThreadClose();
    }
#endif
    PlanState* outer_plan = outerPlanState(node);

    if (outer_plan != NULL)
        ExecEndNode(outer_plan);
}

/*
 * @Description:
 *		Check if the query is stopped or canceled.
 *
 * @return bool : true -- receive stop/cancel signal
 */
bool executorEarlyStop()
{
    /* Check if query already been stopped. */
    if (u_sess->exec_cxt.executorStopFlag == true)
        return true;
    /* Check if query already canceled due to error or cancel/die signal. */
    else if (u_sess->stream_cxt.global_obj && u_sess->stream_cxt.global_obj->isQueryCanceled())
        return true;
    else
        return false;
}

/*
 * @Function: ExecReSetStream()
 *
 * @Brief: Reset the stream state structure in rescan case, for first level of stream
 *         we do thing as the synchronization is guarenteed by RecursiveUnion
 *
 * @Input node: node stream planstate
 *
 * @Return: no return value
 */
void ExecReSetStream(StreamState* node)
{
    Assert(IS_PGXC_DATANODE && node != NULL && (IsA(node, StreamState) || IsA(node, VecStreamState)));

    Plan* plan = node->ss.ps.plan;
    Stream* stream_plan = (Stream*)plan;

    Assert(plan != NULL && IsA(plan, Stream));

    int stream_plan_nodeid = GET_PLAN_NODEID(plan);

    node->isReady = false;
    node->receive_message = false;

    if (stream_plan->is_recursive_local) {
        Assert(stream_plan->smpDesc.distriType == LOCAL_ROUNDROBIN && stream_plan->smpDesc.consumerDop == 1 &&
               stream_plan->smpDesc.producerDop == 1);

        resetLocalStreamContext(node->consumer->getSharedContext());
    }

    /* We dont't have to do specific operations for first level */
    if (IsFirstLevelStreamStateNode(node)) {
        return;
    }

    if (NeedSetupSyncUpController(plan)) {
        StreamNodeGroup* stream_nodegroup = u_sess->stream_cxt.global_obj;
        StreamController* controller = (StreamController*)stream_nodegroup->GetSyncController(stream_plan_nodeid);

        if (controller == NULL) {
            return;
        }

        StreamNodeGroup::SyncConsumerNextPlanStep(stream_plan_nodeid, 0);
    }
    return;
}
