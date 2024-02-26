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
 * streamConsumer.cpp
 *	  Support methods for class streamProducer.
 *
 * IDENTIFICATION
 *	  src/gausskernel/process/stream/streamProducer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/poll.h>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/printtup.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "commands/dbcommands.h"
#include "commands/trigger.h"
#include "distributelayer/streamProducer.h"
#include "distributelayer/streamTransportComm.h"
#include "executor/exec/execStream.h"
#include "executor/executor.h"
#include "executor/node/nodeRecursiveunion.h"
#include "executor/tuptable.h"
#include "gssignal/gs_signal.h"
#include "libcomm/libcomm.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "optimizer/dataskew.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "pgxc/copyops.h"
#include "pgxc/execRemote.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/poolmgr.h"
#include "pgxc/pruningslice.h"
#include "postmaster/postmaster.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/anls_opt.h"
#include "utils/combocid.h"
#include "utils/distribute_test.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/guc_tables.h"
#include "utils/snapmgr.h"
#include "vecexecutor/vecstream.h"

extern void StreamSaveTxnContext(StreamTxnContext* stc);
extern void StreamRestoreTxnContext(StreamTxnContext* stc);

StreamProducer::StreamProducer(
    StreamKey key, PlannedStmt* pstmt, Stream* snode, MemoryContext context, int socketNum, StreamTransType transType)
    : StreamObj(context, STREAM_PRODUCER)
{
    errno_t rc = EOK;

    m_connNum = socketNum;
#ifdef ENABLE_MULTIPLE_NODES
    Assert(m_connNum > 0);
#endif
    m_key = key;
    m_netInit = false;
    m_netProtect = 0;
    m_streamNode = snode;
    m_isDummy = snode->is_dummy;
    m_parallel_desc = snode->smpDesc;
    m_transtype = transType;
    m_transport = NULL;
    m_disQuickLocator = NULL;
    m_sharedContext = NULL;
    m_sharedContextInit = false;
    m_broadcastSize = 0;
    m_threadInit = false;
    m_uniqueSQLId = 0;
    m_uniqueSQLUserId = 0;
    m_uniqueSQLCNId = 0;
    m_globalSessionId.sessionId = 0;
    m_globalSessionId.nodeId = 0;
    m_globalSessionId.seq = 0;
    /* Initialize the origin nodelsit */
    m_originConsumerNodeList = NIL;
    m_originProducerExecNodeList = NIL;
    m_skewState = NULL;
    m_nth = 0;
    m_roundRobinIdx = 0;
    m_distributeIdx = NULL;
    m_distributeKey = NULL;
    m_databaseName = NULL;
    m_userName = NULL;
    m_sessionMemory = NULL;
    m_subProducerList = NULL;
    m_instrStream = NULL;
    m_obsinstr = NULL;
    m_sync_guc_variables = NULL;
    m_params = NULL;
    m_hashFun = NULL;
    m_explain_thread_id = 0;
    m_explain_track = false;
    m_subConsumerList = NULL;
    m_postMasterChildSlot = 0;
    m_queryId = 0;
    m_bitNullLen = 0;
    m_bitNumericLen = 0;
    m_tempBuffer = NULL;
    m_colsType = NULL;
    m_parentSessionid = 0;
    m_parentPlanNodeId = 0;
    m_desc = NULL;
    m_consumerNodes = NULL;
    m_sliceBoundary = NULL;
    m_streamType = STREAM_NONE;
    m_dest = DestNone;
    m_channelCalVecFun = NULL;
    m_channelCalFun = NULL;
    m_hasExprKey = false;
    m_exprkeystate = NULL;
    m_econtext = NULL;
    initStringInfo(&m_tupleBuffer);
    initStringInfo(&m_tupleBufferWithCheck);

    /* use the origianl exec_nodes to setup bucketmap for redistribution case */
    if (EXEC_IN_RECURSIVE_MODE(snode) && snode->origin_consumer_nodes != NULL) {
        m_bucketMap = get_bucketmap_by_execnode(snode->origin_consumer_nodes, pstmt, &m_bucketCnt);
    } else {
        m_bucketMap = get_bucketmap_by_execnode(snode->consumer_nodes, pstmt, &m_bucketCnt);
    }

    rc = memset_s(m_skewMatch, sizeof(int) * BatchMaxSize, 0, sizeof(int) * BatchMaxSize);
    securec_check(rc, "\0", "\0");

    rc = memset_s(m_locator, sizeof(int) * BatchMaxSize, 0, sizeof(int) * BatchMaxSize);
    securec_check(rc, "\0", "\0");

    /*
     * Each Stream thread has a copy of PlannedStmt which comes from top consumer.
     */
    AutoContextSwitch streamCxtGuard(m_memoryCxt);
    m_plan = makeNode(PlannedStmt);
    rc = memcpy_s(m_plan, sizeof(PlannedStmt), pstmt, sizeof(PlannedStmt));
    securec_check(rc, "\0", "\0");
}

StreamProducer::~StreamProducer()
{
    m_plan = NULL;
    m_databaseName = NULL;
    m_userName = NULL;
    m_sessionMemory = NULL;
    m_subProducerList = NULL;
    m_instrStream = NULL;
    m_obsinstr = NULL;
    m_sync_guc_variables = NULL;
    m_sharedContext = NULL;
    m_originConsumerNodeList = NULL;
    m_originProducerExecNodeList = NULL;
    m_subConsumerList = NULL;
    m_tempBuffer = NULL;
    m_colsType = NULL;
    m_desc = NULL;
    m_consumerNodes = NULL;
    m_bucketMap = NULL;
    m_disQuickLocator = NULL;
    m_distributeKey = NULL;
    m_distributeIdx = NULL;
    m_skewState = NULL;
}

/*
 * @Description: Init the stream producer
 *
 * @param[IN] desc:  tuple desc
 * @param[IN] txnCxt:  stream transcation context
 * @param[IN] params:  param list
 * @return: void
 */
void StreamProducer::init(TupleDesc desc, StreamTxnContext txnCxt, ParamListInfo params, int parentPlanNodeId)
{
    AutoContextSwitch streamCxtGuard(m_memoryCxt);

    /*
     * Each Stream thread has a copy of PlannedStmt which comes from top consumer.
     * Differ them by assigning different plan tree(left tree of stream node).
     */
    m_plan->planTree = (Plan*)copyObject(m_streamNode->scan.plan.lefttree);
    m_plan->num_streams = 0;
    m_plan->commandType = CMD_SELECT;
    m_plan->hasReturning = false;
    m_plan->resultRelations = NIL;

    m_databaseName = get_database_name(u_sess->proc_cxt.MyDatabaseId);
    /*  Use the login username but not the current username in stream for inner connection. */
    m_userName = u_sess->proc_cxt.MyProcPort->user_name;
    m_wlmParams = u_sess->wlm_cxt->wlm_params;
    m_explain_thread_id = u_sess->instr_cxt.gs_query_id->procId;
    m_explain_track = u_sess->exec_cxt.need_track_resource;
    m_consumerNodes = (ExecNodes*)copyObject(m_streamNode->consumer_nodes);
    m_sliceBoundary = m_consumerNodes->boundaries;
    m_parentPlanNodeId = parentPlanNodeId;
    m_desc = CreateTupleDescCopyConstr(desc);
#ifdef ENABLE_MULTIPLE_NODES
    m_params = params;
#else
    /* smp in producer should copy paramlist to avoid main thread exit that memory not available. */
    if (u_sess->SPI_cxt._connected >= 0) {
        m_params = copyParamList(params);
    } else {
        m_params = params;
    }
#endif

    m_streamTxnCxt = txnCxt;
    m_streamTxnCxt.CurrentTransactionState =
        (void*)CopyTxnStateByCurrentMcxt((TransactionState)txnCxt.CurrentTransactionState);
    m_streamTxnCxt.snapshot = CopySnapshotByCurrentMcxt(txnCxt.snapshot);

    m_instrStream = u_sess->instr_cxt.global_instr;
    m_obsinstr = u_sess->instr_cxt.obs_instr;

    m_streamType = m_streamNode->type;
    m_channelCalFun = NULL;
    m_channelCalVecFun = NULL;

    m_syncParam.TempNamespace = InvalidOid;
    m_syncParam.TempToastNamespace = InvalidOid;
    m_syncParam.IsBinaryUpgrade = false;
    m_syncParam.CommIpcLog = false;

    /* Set trans type. */
    if (STREAM_IS_LOCAL_NODE(m_parallel_desc.distriType))
        m_transtype = STREAM_MEM;

    bool is_vec_plan = ((Plan*)m_streamNode)->vec_output;

    m_roundRobinIdx = 0;
    SetDest(is_vec_plan);

    if (m_dest == DestBatchRedistribute || m_dest == DestBatchHybrid) {
        int numericCols = 0;
        m_bitNullLen = BITMAPLEN(m_desc->natts);
        m_bitNumericLen = 0;

        /* count of numeric type columns */
        m_colsType = (uint32*)palloc(sizeof(uint32) * (m_desc->natts));

        redistributeStreamInitType(m_desc, m_colsType);

        for (int i = 0; i < m_desc->natts; i++) {
            if (m_desc->attrs[i].atttypid == NUMERICOID)
                numericCols++;
        }

        if (numericCols != 0)
            m_bitNumericLen = BITMAPLEN(2 * numericCols);
        m_tempBuffer = (char*)palloc(sizeof(char) * (m_bitNumericLen + m_bitNullLen));
    }

    /* dummy thread need not connect consumer, need not init stream key either */
    if ((STREAM_COMM == m_transtype) && !m_isDummy)
        initStreamKey();

    m_nodeGroup = u_sess->stream_cxt.global_obj;
    registerGroup();
    m_sync_guc_variables = u_sess->utils_cxt.sync_guc_variables;

    /* flag this stream object as already init. */
    m_init = true;

    STREAM_LOG(DEBUG2,
        "producer inited, %s, StreamKey(%lu, %u, %u), dummy: %s, "
        "consumer nodelist length: %d, connection number: %d",
        GetStreamType(m_streamNode),
        m_key.queryId,
        m_key.planNodeId,
        m_key.smpIdentifier,
        PRINTTRUE(m_isDummy),
        list_length(m_consumerNodes->nodeList),
        m_connNum);
}

/*
 * @Description: Send infomation for redistribute including remote redistribute,
 *				 split redistribute and local redistribute. The m_disQuickLocator
 *				 and hash function for each column is set here.
 *
 * @return: void
 */
void StreamProducer::setDistributeInfo()
{
    int nodeLen = 0;
    int i = 0;
    int j = 0;

    /*
     * If under a recursive CTE execution, we need set the distribution node
     * length properly
     */
    if (EXEC_IN_RECURSIVE_MODE(m_streamNode) && m_originConsumerNodeList != NIL) {
        nodeLen = list_length(m_streamNode->origin_consumer_nodes->nodeList);
    } else {
        nodeLen = list_length(m_consumerNodes->nodeList);
    }

    if (IS_SPQ_RUNNING) {
        nodeLen = m_plan->num_nodes;
    }

    Assert(nodeLen > 0);

    m_disQuickLocator = (uint2**)palloc0(nodeLen * sizeof(uint2*));

    /*
     * we must build the nodes for dn1 dn2 dn3 dn1 dn2 dn3
     * for distribution equality
     *    dn1 dn1  dn2 dn2 dn3 dn3.. is wrong.
     */
    for (i = 0; i < nodeLen; i++) {
        /* distribute to consumer dop thread. */
        m_disQuickLocator[i] = (uint2*)palloc0(sizeof(uint2) * m_parallel_desc.consumerDop);

        /* set the right index. */
        for (j = 0; j < m_parallel_desc.consumerDop; j++)
            m_disQuickLocator[i][j] = i + j * nodeLen;
    }

#ifdef USE_SPQ
    if (m_parallel_desc.distriType != REMOTE_DIRECT_DISTRIBUTE) {
        setDistributeIdx();
    }
#else
    setDistributeIdx();
#endif

    if (((Plan*)m_streamNode)->vec_output == false)
        BindingRedisFunction<false>();
    else
        BindingRedisFunction<true>();
}

/*
 * @Description: Init stream key
 *
 * @return: void
 */
void StreamProducer::initStreamKey()
{
    int nodeLen = list_length(m_consumerNodes->nodeList);
#ifdef USE_SPQ
    nodeLen = m_plan->num_nodes;
#endif
    for (int i = 0; i < m_connNum; i++) {
        StreamCOMM* scomm = (StreamCOMM*)m_transport[i];

        scomm->m_addr->streamKey.queryId = m_key.queryId;
        scomm->m_addr->streamKey.planNodeId = m_key.planNodeId;
        scomm->m_addr->streamKey.producerSmpId = m_key.smpIdentifier;

        if (STREAM_IS_LOCAL_NODE(m_parallel_desc.distriType)) {
            scomm->m_addr->streamKey.consumerSmpId = i;
        } else {
            if (nodeLen == 0)
                ereport(ERROR,
                    (errmodule(MOD_STREAM),
                        errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("Stream consumer nodes should not be null.")));

            scomm->m_addr->streamKey.consumerSmpId = i / nodeLen;
        }
    }
}

void StreamProducer::initSkewState()
{
    StreamSkew* sskew = NULL;
    bool isVec = false;
    m_skewState = NULL;

    /* When this stream is a dummy one, we dont have to init skew state any more. */
    if (m_isDummy)
        return;

    if (m_streamNode->skew_list != NIL) {
        isVec = IsA(&(m_streamNode->scan.plan), VecStream);
        sskew = New(m_memoryCxt) StreamSkew(m_streamNode->skew_list, isVec);
        sskew->init(isVec);
        sskew->m_localNodeId = findLocalChannel();
    }

    if (m_streamNode->type == STREAM_HYBRID && sskew == NULL)
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("No skew quals found for Hybrid Stream\n")));

    m_skewState = (void*)sskew;
}

void StreamProducer::setChildSlot(int childSlot)
{
    m_postMasterChildSlot = childSlot;
}

int StreamProducer::getChildSlot()
{
    return m_postMasterChildSlot;
}

void StreamProducer::setSubProducerList(List* subProducerList)
{
    m_subProducerList = NULL;
    ListCell* cell = NULL;

    foreach (cell, subProducerList) {
        StreamProducer* producer = (StreamProducer*)lfirst(cell);
        /* Just remember the sub producer of current smp worker thread. */
        if (producer->getKey().smpIdentifier == m_key.smpIdentifier)
            m_subProducerList = lappend(m_subProducerList, producer);
    }
}

void StreamProducer::setSubConsumerList(List* subConsumerList)
{
    m_subConsumerList = NULL;
    ListCell* cell = NULL;

    foreach (cell, subConsumerList) {
        StreamConsumer* consumer = (StreamConsumer*)lfirst(cell);
        /* Just remember the sub consumer of current smp worker thread. */
        if (consumer->getKey().smpIdentifier == m_key.smpIdentifier)
            m_subConsumerList = lappend(m_subConsumerList, consumer);
    }
}

void StreamProducer::setSessionMemory(SessionLevelMemory* sessionMemory)
{
    m_sessionMemory = sessionMemory;
}

SessionLevelMemory* StreamProducer::getSessionMemory()
{
    return m_sessionMemory;
}

void StreamProducer::setParentSessionid(uint64 sessionid)
{
    m_parentSessionid = sessionid;
}

uint64 StreamProducer::getParentSessionid()
{
    return m_parentSessionid;
}

List* StreamProducer::getSubProducerList()
{
    return m_subProducerList;
}

int StreamProducer::getParentPlanNodeId()
{
    return m_parentPlanNodeId;
}

struct config_generic** StreamProducer::get_sync_guc_variables()
{
    return m_sync_guc_variables;
}

/*
 * @Description: Set distribute Idx
 *
 * @param[IN] node:  stream node
 * @return: void
 */
void StreamProducer::setDistributeIdx()
{
    int len = list_length(m_streamNode->distribute_keys);
    Assert(len > 0);
    m_distributeKey = m_streamNode->distribute_keys;
    m_distributeIdx = (AttrNumber*)palloc0(len * sizeof(AttrNumber));
    int i = 0;
    ListCell* cell = NULL;

    foreach (cell, m_distributeKey) {
        Node* node = (Node*)lfirst(cell);
        if (!IsA(node, Var)) {
            m_hasExprKey = true;
            m_exprkeystate = ExecInitExprList(m_streamNode->distribute_keys, NULL);
            break;
        }
        Var* distriVar = (Var*)node;
        m_distributeIdx[i++] = distriVar->varattno - 1;
        ereport(DEBUG2, (errmodule(MOD_STREAM), errmsg("[StreamProducer] node id is: %d, distributeIdx[%d] is: %d",
            m_streamNode->scan.plan.plan_node_id, i - 1,  m_distributeIdx[i - 1])));
    }
}

/*
 * @Description: Set shared context for local stream
 *
 * @param[IN] sharedContext: shared context
 * @return: void
 */
void StreamProducer::setSharedContext(StreamSharedContext* sharedContext)
{
    m_sharedContext = sharedContext;
}

/*
 * @Description: Get shared context for local stream
 *
 * @return: StreamSharedContext*
 */
StreamSharedContext* StreamProducer::getSharedContext()
{
    return m_sharedContext;
}

void StreamProducer::setUniqueSQLKey(uint64 unique_sql_id,
    Oid unique_user_id, uint32 unique_cn_id)
{
    m_uniqueSQLId = unique_sql_id;
    m_uniqueSQLUserId = unique_user_id;
    m_uniqueSQLCNId = unique_cn_id;
}

void StreamProducer::getUniqueSQLKey(uint64* unique_id, Oid* user_id, uint32* cn_id)
{
    *unique_id = m_uniqueSQLId;
    *user_id = m_uniqueSQLUserId;
    *cn_id = m_uniqueSQLCNId;
}

void StreamProducer::setGlobalSessionId(GlobalSessionId* globalSessionId)
{
    m_globalSessionId = *globalSessionId;
}

void StreamProducer::getGlobalSessionId(GlobalSessionId* globalSessionId)
{
    *globalSessionId = m_globalSessionId;
}

/*
 * @Description: Get send dest for local stream error info.
 *
 * @return: int
 */
int StreamProducer::getNth()
{
    return m_nth;
}

/*
 * @Description: Building binding function
 *
 * @return: void
 */
template<bool vectorized>
void StreamProducer::BindingRedisFunction()
{
    int len = list_length(m_distributeKey);
    Oid dataType;
    m_hashFun = (hashFun*)palloc0(sizeof(hashFun) * len);
    for (int i = 0; i < len; i++) {
        if (m_hasExprKey) {
            dataType = ((ExprState*)list_nth(m_exprkeystate, i))->resultType;
        } else {
            dataType = m_desc->attrs[m_distributeIdx[i]].atttypid;
        }
        switch (dataType) {
            case INT8OID:
                m_hashFun[i] = &computeHashT<INT8OID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case INT1OID:
                m_hashFun[i] = &computeHashT<INT1OID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case INT2OID:
                m_hashFun[i] = &computeHashT<INT2OID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case OIDOID:
                m_hashFun[i] = &computeHashT<OIDOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case INT4OID:
                m_hashFun[i] = &computeHashT<INT4OID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case BOOLOID:
                m_hashFun[i] = &computeHashT<BOOLOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case CHAROID:
                m_hashFun[i] = &computeHashT<CHAROID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case NAMEOID:
                m_hashFun[i] = &computeHashT<NAMEOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case INT2VECTOROID:
                m_hashFun[i] = &computeHashT<INT2VECTOROID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case NVARCHAR2OID:
                m_hashFun[i] = &computeHashT<NVARCHAR2OID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case VARCHAROID:
                m_hashFun[i] = &computeHashT<VARCHAROID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case CLOBOID:
                m_hashFun[i] = &computeHashT<CLOBOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case TEXTOID:
                m_hashFun[i] = &computeHashT<TEXTOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case OIDVECTOROID:
                m_hashFun[i] = &computeHashT<OIDVECTOROID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case FLOAT4OID:
                m_hashFun[i] = &computeHashT<FLOAT4OID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case FLOAT8OID:
                m_hashFun[i] = &computeHashT<FLOAT8OID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case ABSTIMEOID:
                m_hashFun[i] = &computeHashT<ABSTIMEOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case RELTIMEOID:
                m_hashFun[i] = &computeHashT<RELTIMEOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case CASHOID:
                m_hashFun[i] = &computeHashT<CASHOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case BPCHAROID:
                m_hashFun[i] = &computeHashT<BPCHAROID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case RAWOID:
                m_hashFun[i] = &computeHashT<RAWOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case BYTEAWITHOUTORDERWITHEQUALCOLOID:
                m_hashFun[i] = &computeHashT<BYTEAWITHOUTORDERWITHEQUALCOLOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case BYTEAWITHOUTORDERCOLOID:
                m_hashFun[i] = &computeHashT<BYTEAWITHOUTORDERCOLOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case BYTEAOID:
                m_hashFun[i] = &computeHashT<BYTEAOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case DATEOID:
                m_hashFun[i] = &computeHashT<DATEOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case TIMEOID:
                m_hashFun[i] = &computeHashT<TIMEOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case TIMESTAMPOID:
                m_hashFun[i] = &computeHashT<TIMESTAMPOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case TIMESTAMPTZOID:
                m_hashFun[i] = &computeHashT<TIMESTAMPTZOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case INTERVALOID:
                m_hashFun[i] = &computeHashT<INTERVALOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case TIMETZOID:
                m_hashFun[i] = &computeHashT<TIMETZOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case SMALLDATETIMEOID:
                m_hashFun[i] = &computeHashT<SMALLDATETIMEOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case NUMERICOID:
                m_hashFun[i] = &computeHashT<NUMERICOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            case UUIDOID:
                m_hashFun[i] = &computeHashT<UUIDOID, LOCATOR_TYPE_HASH, vectorized>;
                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("Unhandled datatype for modulo or hash distribution\n")));
        }
    }

    if (vectorized)
        DispatchBatchRedistrFunction(len);
    else
        DispatchRowRedistrFunction(len);
}

/*
 * @Description: Dispatch batch sending function
 *
 * @param[IN] len:  number of distribute key
 * @return: void
 */
void StreamProducer::DispatchBatchRedistrFunction(int len)
{
    if (m_sliceBoundary == NULL) {
        switch (len) {
            case 1:
                DispatchBatchRedistrFunctionByRedisType<1>();
                break;

            case 2:
                DispatchBatchRedistrFunctionByRedisType<2>();
                break;

            case 3:
            default:
                DispatchBatchRedistrFunctionByRedisType<3>();
                break;
        }
    } else {
        /* range/list batch redistribute */
        DispatchBatchRedistrFunctionForSlice();
    }
}

/*
 * @Description: Dispatch tuple sending function
 *
 * @param[IN] len:  number of distribute key
 * @return: void
 */
void StreamProducer::DispatchRowRedistrFunction(int len)
{
    if (m_sliceBoundary == NULL) {
        switch (len) {
            case 1:
                DispatchRowRedistrFunctionByRedisType<1>();
                break;

            case 2:
                DispatchRowRedistrFunctionByRedisType<2>();
                break;

            case 3:
            default:
                DispatchRowRedistrFunctionByRedisType<3>();
                break;
        }
    } else {
        /* range/list redistribute */
        DispatchRowRedistrFunctionForSlice();
    }
}

/*
 * @Description: Enter critical section for net protect purpose
 *
 * @param[IN] func:  pointer to the function need protect
 * @return: void
 */
void StreamProducer::enterCriticalSection(criticalSectionFunc func)
{
    volatile sig_atomic_t* pTarget = NULL;
    sig_atomic_t value;
    pTarget = &m_netProtect;

    HOLD_INTERRUPTS();
    do {
        if (*pTarget == 0) {
            /*  perform an atomic compare and swap. */
            value = __sync_val_compare_and_swap(pTarget, 0, 1);
            if (value == 0) {
                (this->*func)();

                /*  restore the value. */
                m_netProtect = 0;
                break;
            }
        }

        pg_usleep(100);
    } while (true);
    RESUME_INTERRUPTS();
}

/*
 * @Description: Deinit the producer
 *
 * @param[IN] status:  object status
 * @return: void
 */
void StreamProducer::deInit(StreamObjStatus status)
{
    m_status = status;

    if (status == STREAM_ERROR) {
        ListCell* cell = NULL;
        /* release un initilize sub producer object. */
        foreach (cell, m_subProducerList) {
            StreamProducer* pro = (StreamProducer*)lfirst(cell);
            pro->releaseUninitializeResourceWithProtect();
        }
    }

    if (m_init == false)
        return;

    if (m_transtype == STREAM_COMM && status == STREAM_ERROR) {
    retry:
        /*
         * If close message(control message via TCP) is faster than error
         * message('E' message), the consumer will detect the
         * remote close before receiving error message. Add 1 sec sleep time
         * to make sure error message can arrive at consumer side normally.
         * It is quite tricky to some extent, but the timing issue can be solved
         * in most cases.
         */

        int ret = usleep(1000000); /* 1 sec */

        /* usleep will be interrupted, so we retry when sleep less then 1 sec */
        if ((ret == -1) && (errno == EINTR))
            goto retry;
        enterCriticalSection(&StreamObj::releaseNetPort);
    }

    /* need protect for probable concurrent deInit of parent producer */
    enterCriticalSection(&StreamProducer::releaseSubConsumerList);

    if (StreamThreadAmI() && u_sess->proc_cxt.MyProcPort != NULL) {
        /* Set the sock to -1, in case close other thread fd */
        u_sess->proc_cxt.MyProcPort->sock = -1;

        /* Both are shallow-copy pointers, reset to NULL before thread exit */
        u_sess->proc_cxt.MyProcPort->database_name = NULL;
        u_sess->proc_cxt.MyProcPort->user_name = NULL;
    }

    /* release skew state */
    if (m_skewState != NULL) {
        delete (StreamSkew*)m_skewState;
        m_skewState = NULL;
    }

    m_nodeGroup->unregisterStream(m_nodeGroupIdx, status);

    /* flag the object as uninit. */
    m_init = false;

    m_originConsumerNodeList = NIL;
    m_originProducerExecNodeList = NIL;
}

/*
 * @Description: Release net port of sub consumers
 *
 * @return: void
 */
void StreamProducer::releaseSubConsumerList()
{
    ListCell* cell = NULL;
    foreach (cell, m_subConsumerList) {
        StreamConsumer* con = (StreamConsumer*)lfirst(cell);
        con->deInit();
    }
}

/*
 * @Description: Release net port with protect
 *
 * @return: void
 */
void StreamProducer::releaseUninitializeResourceWithProtect()
{
    enterCriticalSection(&StreamProducer::releaseUninitializeResource);
}

/*
 * @Description: Release net port if net is not init yet
 *
 * @return: void
 */
void StreamProducer::releaseUninitializeResource()
{
    /*  just release uninitialized sub producer object. */
    if (!m_netInit) {
        releaseNetPort();
        releaseSubConsumerList();
    }
}

/*
 * @Description: Init the net port
 *
 * @return: void
 */
void StreamProducer::netPortInit()
{
    if (m_transport != NULL) {
        for (int i = 0; i < m_connNum; i++) {
            if (m_transport[i])
                m_transport[i]->init(getDbName(), getUserName());
        }

        m_netInit = true;
    }
}

/*
 * @Description: Init the net environment
 *
 * @return: void
 */
void StreamProducer::netInit()
{
    /* init tuple buffer. */
    initStringInfo(&m_tupleBuffer);

    initStringInfo(&m_tupleBufferWithCheck);

    /* Local stream do not need net buffer. */
    if (STREAM_IS_LOCAL_NODE(m_streamNode->smpDesc.distriType)) {
        /* Init shared context for local stream */
        u_sess->stream_cxt.producer_obj->initSharedContext();
        return;
    }

    if (m_transport != NULL) {
        for (int i = 0; i < m_connNum; i++) {
            if (m_transport[i])
                m_transport[i]->allocNetBuffer();
        }
    }

    /* init address info and key */
    enterCriticalSection(&StreamProducer::netPortInit);
}

/*
 * @Description: Register producer thread into thread node group, it should be call in the stream thread
 *
 * @return: void
 */
void StreamProducer::registerGroup()
{
    m_nodeGroupIdx = m_nodeGroup->registerStream(this);
}

/*
 * @Description: Switch the send direction to the nth channel
 *
 * @param[IN] nthChannel:  channel index
 * @return: true if switch successfully
 */
bool StreamProducer::netSwitchDest(int nthChannel)
{
    if (m_netInit) {
        if (m_transport[nthChannel]->setActive())
            return true;
        else
            return false;
    }
    return false;
}

/*
 * @Description: Save some network status for next sending
 *
 * @param[IN] nthChannel:  channel index
 * @return: void
 */
void StreamProducer::netStatusSave(int nthChannel)
{
    m_transport[nthChannel]->setInActive();
}

/*
 * @Description: Send tuple with BroadCast method to local consumers.
 *
 * @param[IN] tuple: tuple slot
 * @param[IN] self: receiver
 * @return: void
 */
void StreamProducer::localBroadCastStream(TupleTableSlot* tuple)
{
    for (int i = 0; i < m_connNum; i++) {
        sendByMemory(tuple, NULL, i);
    }
}

/*
 * @Description: Send batch with BroadCast method to local consumers.
 *
 * @param[IN] batch: vector batches
 * @return: void
 */
void StreamProducer::localBroadCastStream(VectorBatch* batchSrc)
{
    for (int i = 0; i < m_connNum; i++) {
        sendByMemory(NULL, batchSrc, i);
    }
}

/*
 * @Description: Send tuple with Redistribute method to local consumers.
 *
 * @param[IN] tuple: tuple slot
 * @return: void
 */
void StreamProducer::localRedistributeStream(TupleTableSlot* tuple)
{
    Assert(m_sharedContext != NULL);

    (this->*m_channelCalFun)(tuple);

    sendByMemory(tuple, NULL, m_locator[0]);
}

/*
 * @Description: Send batch with Redistribute method to local consumers.
 *
 * @param[IN] batch: vector batches
 * @return: void
 */
void StreamProducer::localRedistributeStream(VectorBatch* batch)
{
    Assert(m_sharedContext != NULL);

    (this->*m_channelCalVecFun)(batch);

    for (int i = 0; i < batch->m_rows; i++) {
        sendByMemory(NULL, batch, m_locator[i], i);
    }
}

/*
 * @Description: Send tuple with Roundrobin method to local consumer by memory.
 *
 * @param[IN] tuple:  tuple slot
 * @return: void
 */
void StreamProducer::localRoundRobinStream(TupleTableSlot* tuple)
{
    sendByMemory(tuple, NULL, m_roundRobinIdx);

    /* Update roundrobin index. */
    m_roundRobinIdx++;
    m_roundRobinIdx = m_roundRobinIdx % m_connNum;
}

/*
 * @Description: Send batch with Roundrobin method to local consumer by memory.
 *
 * @param[IN] batch:  vector batch
 * @param[IN] self:  receiver
 * @return: void
 */
void StreamProducer::localRoundRobinStream(VectorBatch* batch)
{
    sendByMemory(NULL, batch, m_roundRobinIdx);

    /* Update roundrobin index. */
    m_roundRobinIdx++;
    m_roundRobinIdx = m_roundRobinIdx % m_connNum;
}

/*
 * @Description: Calculate the destinations for data in a batch.
 *
 * @param[IN] batch:  vector batch
 * @return: void
 */
template<int keyNum, int distrType>
void StreamProducer::redistributeBatchChannel(VectorBatch* batch)
{
    /*
    * For dn gather case, we do not need to compute hash value.
    * we only has one execute datanode in consumer list.
    * So, send and receive channel will always be channel 0.
    */
    if (distrType == REMOTE_DIRECT_DISTRIBUTE) {
        return;
    }

    ScalarVector* pDistributeVec = NULL;
    uint64 hashValue[BatchMaxSize] = {0};
    bool isNull[BatchMaxSize] = {true};
    Datum data;

    Assert((BUCKETDATALEN & (BUCKETDATALEN - 1)) == 0);
    Assert(m_disQuickLocator != NULL);

    if (keyNum >= 1) {
        pDistributeVec = &batch->m_arr[m_distributeIdx[0]];
        for (int i = 0; i < batch->m_rows; i++) {
            if (!pDistributeVec->IsNull(i)) {
                data = pDistributeVec->m_vals[i];
                hashValue[i] = m_hashFun[0](data);
                isNull[i] = false;
            } else {
                isNull[i] = true;
            }
        }
    }

    if (keyNum >= 2) {
        pDistributeVec = &batch->m_arr[m_distributeIdx[1]];
        for (int i = 0; i < batch->m_rows; i++) {
            if (!pDistributeVec->IsNull(i)) {
                data = pDistributeVec->m_vals[i];
                if (!isNull[i]) {
                    hashValue[i] = (hashValue[i] << 1) | ((hashValue[i] & 0x80000000) ? 1 : 0);
                    hashValue[i] ^= m_hashFun[1](data);
                } else {
                    hashValue[i] = m_hashFun[1](data);
                    isNull[i] = false;
                }
            }
        }
    }

    /* Handle when the number of distribute key > 2. */
    if (keyNum == 3) {
        int redistributeKeyNum = list_length(m_distributeKey);
        for (int j = 2; j < redistributeKeyNum; j++) {
            pDistributeVec = &batch->m_arr[m_distributeIdx[j]];

            for (int i = 0; i < batch->m_rows; i++) {
                if (!pDistributeVec->IsNull(i)) {
                    data = pDistributeVec->m_vals[i];
                    if (!isNull[i]) {
                        hashValue[i] = (hashValue[i] << 1) | ((hashValue[i] & 0x80000000) ? 1 : 0);
                        hashValue[i] ^= m_hashFun[j](data);
                    } else {
                        hashValue[i] = m_hashFun[j](data);
                        isNull[i] = false;
                    }
                }
            }
        }
    }

    int dop = m_parallel_desc.consumerDop;
    int nodeLen = list_length(m_consumerNodes->nodeList);
    for (int i = 0; i < batch->m_rows; i++) {
        m_locator[i] = ChannelLocalizer<distrType>(hashValue[i], dop, nodeLen);
    }
}

template<int distrType>
void StreamProducer::redistributeBatchChannelForSlice(VectorBatch* batch)
{
    if (distrType == REMOTE_DIRECT_DISTRIBUTE) {
        return;
    }

    int keyNum;
    Datum keyValues[MAX_RANGE_PARTKEY_NUMS];
    bool keyNulls[MAX_RANGE_PARTKEY_NUMS];
    Oid KeyAttrs[MAX_RANGE_PARTKEY_NUMS];
    int colMap[MAX_RANGE_PARTKEY_NUMS];
    Datum data;
    uint64 hashValue;
    bool allIsNull;
    ScalarVector* pDistributeVec = NULL;
    Const consts[MAX_RANGE_PARTKEY_NUMS];
    Const* constPointers[MAX_RANGE_PARTKEY_NUMS] = {NULL};

    keyNum = list_length(m_distributeKey);

    for (int i = 0; i < batch->m_rows; i++) {
        allIsNull = true;
        hashValue = 0;

        for (int j = 0; j < keyNum; j++) {
            pDistributeVec = &batch->m_arr[m_distributeIdx[j]];
            data = pDistributeVec->m_vals[i];
            keyNulls[j] = pDistributeVec->IsNull(i);
            KeyAttrs[j] = m_desc->attrs[m_distributeIdx[j]].atttypid;
            keyValues[j] = data;
            colMap[j] = j;

            if (!keyNulls[j]) {
                if (!allIsNull) {
                    hashValue = (hashValue << 1) | ((hashValue & 0x80000000) ? 1 : 0);
                    hashValue ^= m_hashFun[j](data);
                } else {
                    hashValue = m_hashFun[j](data);
                    allIsNull = false;
                }
            }
        }
        ConstructConstFromValues(keyValues, keyNulls, KeyAttrs, colMap, keyNum, consts, constPointers);

        m_locator[i] = ChannelLocalizerForSlice<distrType>(
            hashValue, constPointers, m_parallel_desc.consumerDop);
    }
}

/*
 * @Description: Calculate the destination consumer for a tuple
 *
 * @param[IN] tuple:  tuple slot
 * @return: void
 */
template<int keyNum, int distrType>
void StreamProducer::redistributeTupleChannel(TupleTableSlot* tuple)
{
    /*
    * For dn gather case, we do not need to compute hash value.
    * we only has one execute datanode in consumer list.
    * So, send and receive channel will always be channel 0.
    */
    if (distrType == REMOTE_DIRECT_DISTRIBUTE) {
        return;
    }

    bool isNull = false;
    Datum data;
    bool allIsNULL = true;
    uint64 hashValue = 0;
	
    if (keyNum >= 1) {
        data = tableam_tslot_getattr(tuple, m_distributeIdx[0] + 1, &isNull);
        if (!isNull) {
            hashValue = m_hashFun[0](data);
            allIsNULL = false;
        }
    }

    if (keyNum >= 2) {
        data = tableam_tslot_getattr(tuple, m_distributeIdx[1] + 1, &isNull);

        if (!isNull) {
            if (!allIsNULL) {
                hashValue = (hashValue << 1) | ((hashValue & 0x80000000) ? 1 : 0);
                hashValue ^= m_hashFun[1](data);
            } else {
                hashValue = m_hashFun[1](data);
                allIsNULL = false;
            }
        }
    }

    /* Handle when the number of distribute key > 2. */
    if (keyNum == 3) {
        int len = list_length(m_distributeKey);
        for (int i = 2; i < len; i++) {
            data = tableam_tslot_getattr(tuple, m_distributeIdx[i] + 1, &isNull);
            if (!isNull) {
                if (!allIsNULL) {
                    hashValue = (hashValue << 1) | ((hashValue & 0x80000000) ? 1 : 0);
                    hashValue ^= m_hashFun[i](data);
                } else {
                    hashValue = m_hashFun[i](data);
                    allIsNULL = false;
                }
            }
        }
    }

    m_locator[0] = ChannelLocalizer<distrType>(
        hashValue, m_parallel_desc.consumerDop, list_length(m_consumerNodes->nodeList));
}

template<int distrType>
void StreamProducer::redistributeTupleChannelWithExpr(TupleTableSlot* tuple)
{
    /*
    * For dn gather case, we do not need to compute hash value.
    * we only has one execute datanode in consumer list.
    * So, send and receive channel will always be channel 0.
     */
    if (distrType == REMOTE_DIRECT_DISTRIBUTE) {
        return;
    }

    Datum data;
    MemoryContext oldContext;
    ListCell *cell;
    bool isNull = false;
    bool allIsNULL = true;
    uint64 hashValue = 0;
    int i = 0;
    m_econtext->ecxt_outertuple = tuple;

    oldContext = MemoryContextSwitchTo(m_econtext->ecxt_per_tuple_memory);
    /* foreach key exprs */
    foreach(cell, m_exprkeystate) {
        ExprState *state = (ExprState*)lfirst(cell);
        data = ExecEvalExpr(state, m_econtext, &isNull, NULL);
        if (!isNull) {
            if (!allIsNULL) {
                hashValue = (hashValue << 1) | ((hashValue & 0x80000000) ? 1 : 0);
                hashValue ^= m_hashFun[i](data);
            } else {
                hashValue = m_hashFun[i](data);
                allIsNULL = false;
            }
        }
        ++i;
    }

    MemoryContextSwitchTo(oldContext);

    m_locator[0] = ChannelLocalizer<distrType>(
        hashValue, m_parallel_desc.consumerDop, list_length(m_consumerNodes->nodeList));
}

template<int distrType>
void StreamProducer::redistributeTupleChannelForSlice(TupleTableSlot* tuple)
{
    int keyNum;
    Datum keyValues[MAX_RANGE_PARTKEY_NUMS] = {0};
    bool keyNulls[MAX_RANGE_PARTKEY_NUMS] = {false};
    Oid keyAttrs[MAX_RANGE_PARTKEY_NUMS] = {0};
    int colMap[MAX_RANGE_PARTKEY_NUMS] = {0};
    Datum data;
    uint64 hashValue = 0;
    bool isNull = false;
    bool allIsNULL = true;
    Const consts[MAX_RANGE_PARTKEY_NUMS];
    Const* constPointers[MAX_RANGE_PARTKEY_NUMS] = {NULL};

    if (distrType == REMOTE_DIRECT_DISTRIBUTE) {
        return;
    }

    keyNum = list_length(m_distributeKey);
    /* calculate hash value */
    for (int i = 0; i < keyNum; i++) {
        data = tableam_tslot_getattr(tuple, m_distributeIdx[i] + 1, &isNull);
        if (!isNull) {
            if (!allIsNULL) {
                hashValue = (hashValue << 1) | ((hashValue & 0x80000000) ? 1 : 0);
                hashValue ^= m_hashFun[i](data);
            } else {
                hashValue = m_hashFun[i](data);
                allIsNULL = false;
            }
        }
    }

    /* calculate distribute key Const values */
    for (int i = 0; i < keyNum; i++) {
        colMap[i] = i;
        keyValues[i] = tableam_tslot_getattr(tuple, m_distributeIdx[i] + 1, &keyNulls[i]);
        keyAttrs[i] = m_desc->attrs[m_distributeIdx[i]].atttypid;
    }
    ConstructConstFromValues(keyValues, keyNulls, keyAttrs, colMap, keyNum, consts, constPointers);

    m_locator[0] = ChannelLocalizerForSlice<distrType>(
        hashValue, constPointers, m_parallel_desc.consumerDop);
}

/*
 * @Description: Dispatch batch sending function by redistribute type
 *
 * @return: void
 */
template<int len>
void StreamProducer::DispatchBatchRedistrFunctionByRedisType()
{
    switch (m_parallel_desc.distriType) {
        case PARALLEL_NONE:
#ifdef ENABLE_MULTIPLE_NODES
        case REMOTE_DISTRIBUTE:
            m_channelCalVecFun = (list_length(m_consumerNodes->nodeList) == 1) ?
                                 &StreamProducer::redistributeBatchChannel<len, REMOTE_DIRECT_DISTRIBUTE> :
                                 &StreamProducer::redistributeBatchChannel<len, REMOTE_DISTRIBUTE>;
            break;
        case REMOTE_SPLIT_DISTRIBUTE:
            m_channelCalVecFun = &StreamProducer::redistributeBatchChannel<len, REMOTE_SPLIT_DISTRIBUTE>;
            break;
#endif
        case LOCAL_DISTRIBUTE:
            m_channelCalVecFun = &StreamProducer::redistributeBatchChannel<len, LOCAL_DISTRIBUTE>;
            break;

        default:
            break;
    }
}

/*
 * @Description: Dispatch tuple sending function by redistribute type
 *
 * @return: void
 */
template<int len>
void StreamProducer::DispatchRowRedistrFunctionByRedisType()
{
    switch (m_parallel_desc.distriType) {
        case PARALLEL_NONE:
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
        case REMOTE_DISTRIBUTE:
            if (m_hasExprKey) {
                m_channelCalFun = ((list_length(m_consumerNodes->nodeList) == 1) ?
                                  &StreamProducer::redistributeTupleChannelWithExpr<REMOTE_DIRECT_DISTRIBUTE> :
                                  &StreamProducer::redistributeTupleChannelWithExpr<REMOTE_DISTRIBUTE>);
            } else {
                m_channelCalFun = ((list_length(m_consumerNodes->nodeList) == 1) ?
                                  &StreamProducer::redistributeTupleChannel<len, REMOTE_DIRECT_DISTRIBUTE> :
                                  &StreamProducer::redistributeTupleChannel<len, REMOTE_DISTRIBUTE>);
            }
            break;

        case REMOTE_SPLIT_DISTRIBUTE:
            if (m_hasExprKey) {
                m_channelCalFun = &StreamProducer::redistributeTupleChannelWithExpr<REMOTE_SPLIT_DISTRIBUTE>;
            } else {
                m_channelCalFun = &StreamProducer::redistributeTupleChannel<len, REMOTE_SPLIT_DISTRIBUTE>;
            }
            break;
        case REMOTE_DIRECT_DISTRIBUTE:
            // REMOTE_DIRECT_DISTRIBUTE will not calculate distribute key
            m_channelCalFun = &StreamProducer::redistributeTupleChannel<len, REMOTE_DIRECT_DISTRIBUTE>;
            break;
#endif
        case LOCAL_DISTRIBUTE:
            if (m_hasExprKey) {
                m_channelCalFun = &StreamProducer::redistributeTupleChannelWithExpr<LOCAL_DISTRIBUTE>;
            } else {
                m_channelCalFun = &StreamProducer::redistributeTupleChannel<len, LOCAL_DISTRIBUTE>;
            }
            break;

        default:
            break;
    }
}

void StreamProducer::DispatchBatchRedistrFunctionForSlice()
{
    switch (m_parallel_desc.distriType) {
        case PARALLEL_NONE:
#if defined(ENABLE_MULTIPLE_NODES) || defined(USE_SPQ)
        case REMOTE_DISTRIBUTE:
            m_channelCalVecFun = ((list_length(m_consumerNodes->nodeList) == 1) ?
                                 &StreamProducer::redistributeBatchChannelForSlice<REMOTE_DIRECT_DISTRIBUTE> :
                                 &StreamProducer::redistributeBatchChannelForSlice<REMOTE_DISTRIBUTE>);
            break;
        case REMOTE_SPLIT_DISTRIBUTE:
            m_channelCalVecFun = &StreamProducer::redistributeBatchChannelForSlice<REMOTE_SPLIT_DISTRIBUTE>;
            break;
#endif
        case LOCAL_DISTRIBUTE:
            m_channelCalVecFun = &StreamProducer::redistributeBatchChannelForSlice<LOCAL_DISTRIBUTE>;
            break;
        default:
            break;
    }
}

void StreamProducer::DispatchRowRedistrFunctionForSlice()
{
    switch (m_parallel_desc.distriType) {
        case PARALLEL_NONE:
#ifdef ENABLE_MULTIPLE_NODES
        case REMOTE_DISTRIBUTE:
            m_channelCalFun = ((list_length(m_consumerNodes->nodeList) == 1) ? 
                              &StreamProducer::redistributeTupleChannelForSlice<REMOTE_DIRECT_DISTRIBUTE> : 
                              &StreamProducer::redistributeTupleChannelForSlice<REMOTE_DISTRIBUTE>);
            break;
        case REMOTE_SPLIT_DISTRIBUTE:
            m_channelCalFun = &StreamProducer::redistributeTupleChannelForSlice<REMOTE_SPLIT_DISTRIBUTE>;
            break;
#endif
        case LOCAL_DISTRIBUTE:
            m_channelCalFun = &StreamProducer::redistributeTupleChannelForSlice<LOCAL_DISTRIBUTE>;
            break;
        default:
            break;
    }
}

template<int distrType>
int StreamProducer::ChannelLocalizerForSlice(ScalarValue hashValue, Const** distValues, int dop)
{
    int nodeIdx = 0;
    int threadIdx = 0;

    Assert(m_disQuickLocator != NULL);

    switch (distrType) {
        case PARALLEL_NONE:
#ifdef ENABLE_MULTIPLE_NODES
        case REMOTE_DISTRIBUTE:
            nodeIdx = NodeLocalizerForSlice(distValues);
            threadIdx = 0;
            break;
        case REMOTE_SPLIT_DISTRIBUTE:
            nodeIdx = NodeLocalizerForSlice(distValues);
            threadIdx = ThreadLocalizerForSlice(hashValue, dop);
            break;
#endif
        case LOCAL_DISTRIBUTE:
            nodeIdx = 0;
            threadIdx = ThreadLocalizerForSlice(hashValue, dop);
            break;
        default:
            Assert(false);
            break;
    }

    return m_disQuickLocator[nodeIdx][threadIdx];
}


/*
 * @Description	: Add CRC check infomation for the data received from stream.
 * @in nthChannel	: The stream channel
 */
void StreamProducer::AddCheckInfo(int nthChannel)
{
    AddCheckMessage(&m_tupleBufferWithCheck, &m_tupleBuffer, true, m_key.planNodeId);
    m_transport[nthChannel]->send(
        m_tupleBufferWithCheck.cursor, m_tupleBufferWithCheck.data, m_tupleBufferWithCheck.len);
    resetStringInfo(&m_tupleBufferWithCheck);
}

/*
 * @Description: Flush the data in the buffer
 *
 * @return: void
 */
void StreamProducer::flushStream()
{
    for (int i = 0; i < m_connNum; i++) {
        if (netSwitchDest(i)) {
            Assert(m_transport && m_transport[i]);

            m_transport[i]->flush();
            netStatusSave(i);
        }
    }
}

char* StreamProducer::getDbName()
{
    return m_databaseName;
}

char* StreamProducer::getUserName()
{
    return m_userName;
}

WLMGeneralParam StreamProducer::getWlmParams()
{
    return m_wlmParams;
}

uint32 StreamProducer::getExplainThreadid()
{
    return m_explain_thread_id;
}

unsigned char StreamProducer::getExplainTrack()
{
    return m_explain_track;
}

PlannedStmt* StreamProducer::getPlan()
{
    return m_plan;
}

bool StreamProducer::isDummy()
{
    return m_isDummy;
}

bool StreamProducer::isLocalStream()
{
    if (STREAM_IS_LOCAL_NODE(m_streamNode->smpDesc.distriType))
        return true;
    else
        return false;
}

CommandDest StreamProducer::getDest()
{
    return m_dest;
}

/*
 * @Description: Set up the write transaction status for the stream thread
 *
 * @return: void
 */
void StreamProducer::setUpStreamTxnEnvironment()
{
    /*  resotre transaction context. */
    StreamRestoreTxnContext(&m_streamTxnCxt);

    /*  transaction id. */
    SetNextTransactionId(m_streamTxnCxt.txnId, false);
    StreamTxnContextSetTransactionState(&m_streamTxnCxt);

    /*  snapshot. */
    copySnapShot();
    if (getSnapShot()) {
        Snapshot snapshot = getSnapShot();
        SetGlobalSnapshotData(snapshot->xmin, snapshot->xmax, snapshot->snapshotcsn, snapshot->timeline, false);
        StreamTxnContextSetSnapShot(snapshot);
        StreamTxnContextSetMyPgXactXmin(snapshot->xmin);
    }

    /*  command id. */
    SaveReceivedCommandId(m_streamTxnCxt.currentCommandId);

    /*  timestamp. */
    SetCurrentGTMDeltaTimestamp();
}

StreamInstrumentation* StreamProducer::getStreamInstrumentation()
{
    return m_instrStream;
}

OBSInstrumentation* StreamProducer::getOBSInstrumentation()
{
    return m_obsinstr;
}

/*
 * @Description: Report error to consumer node
 *
 * @return: void
 */
void StreamProducer::reportError()
{
    stream_send_message_to_server_log();

    m_nodeGroup->saveProducerEdata();

    /* For dummy stream, we create producer only for initing child stream thread. */
    if (STREAM_IS_LOCAL_NODE(m_parallel_desc.distriType) && !m_isDummy) {
        for (int i = 0; i < m_connNum; i++) {
            if (m_sharedContextInit) {
                m_nth = i;
                stream_send_message_to_consumer();
            } else {
                gs_memory_disconnect(m_sharedContext, i);
            }
        }
    } else {
        if (m_transport != NULL) {
            for (int i = 0; i < m_connNum; i++) {
                if (netSwitchDest(i)) {
                    stream_send_message_to_consumer();
                    netStatusSave(i);
                }
            }
        }
    }
}

void StreamProducer::copySnapShot()
{
    MemoryContext oldcxt;

    if (m_streamTxnCxt.snapshot) {
        /*  copy snapshot to thread-self memory context. */
        oldcxt = MemoryContextSwitchTo(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR));
        Snapshot snapshot = CopySnapshotByCurrentMcxt(m_streamTxnCxt.snapshot);
        MemoryContextSwitchTo(oldcxt);

        m_streamTxnCxt.snapshot = snapshot;
    }
}

Snapshot StreamProducer::getSnapShot()
{
    return m_streamTxnCxt.snapshot;
}

ParamListInfo StreamProducer::getParams()
{
    return m_params;
}

/*
 * @Description: Wait thread ID ready
 */
void StreamProducer::waitThreadIdReady()
{
    int ntimes = 1;

    /*
     * If stream thread ID is invalid, wait thread ID ready here so that we can judge
     * whether two threads are in same node group(see StreamNodeGroup::inNodeGroup).
     */
    while (u_sess->stream_cxt.producer_obj->getThreadId() == InvalidTid) {
        /* sleep 1ms */
        pg_usleep(1000);

        ntimes++;
        if (ntimes == 30000) {
            /* this should never happen, system may be completely in a mess */
            ereport(ERROR,
                (errmodule(MOD_STREAM),
                    errcode(ERRCODE_DATA_EXCEPTION),
                    errmsg("stream thread ID has not been set by parent thread after 30s.")));
        }
    }
#ifdef __aarch64__
    pg_memory_barrier();
#endif
}

/*
 * @Description: Choose which node to send by hash value
 *
 * @param[IN] hashValue:  hash value
 * @return: node idx
 */
inline uint2 StreamProducer::NodeLocalizer(ScalarValue hashValue)
{
    return m_bucketMap[(uint32)abs((int)hashValue) & (uint32)(m_bucketCnt - 1)];
}

/*
 * @Description: Choose which node to send by values for range/list redistribution
 *
 * @param[IN]: distValues
 * @return: node idx
 */
inline uint2 StreamProducer::NodeLocalizerForSlice(Const** distValues)
{
    int distLen = list_length(m_distributeKey);
    return GetTargetConsumerNodeIdx(m_sliceBoundary, distValues, distLen);
}

/*
 * @Description: Choose which thread to send by hash value
 *
 * @param[IN] hashValue:  hash value
 * @param[IN] dop:  dop
 * @return: thread idx
 */
inline int StreamProducer::ThreadLocalizer(ScalarValue hashValue, int dop)
{
    return (hashValue / BUCKETDATALEN) % dop;
}

inline int StreamProducer::ThreadLocalizerForSlice(ScalarValue hashValue, int dop) const
{
    return (hashValue / BUCKETDATALEN) % dop;
}


/*
 * @Description: Choose which channel to send by hash value
 *
 * @param[IN] hashValue:  hash value
 * @param[IN] allIsNULL:  if all value is null
 * @param[IN] dop:  dop
 * @param[IN] nodeSize:  global plan id
 * @return: channel idx
 */
template<int distrType>
int StreamProducer::ChannelLocalizer(ScalarValue hashValue, int dop, int nodeSize)
{
    int nodeIdx = 0;
    int threadIdx = 0;

    Assert(m_disQuickLocator != NULL);

    switch (distrType) {
        case PARALLEL_NONE:
        case REMOTE_DISTRIBUTE: {
            nodeIdx = NodeLocalizer(hashValue);
            threadIdx = 0;
        } break;
        case REMOTE_SPLIT_DISTRIBUTE: {
            nodeIdx = NodeLocalizer(hashValue);
            threadIdx = ThreadLocalizer(hashValue, dop);
        } break;
        case LOCAL_DISTRIBUTE: {
            nodeIdx = 0;
            threadIdx = ThreadLocalizer(hashValue, dop);
        } break;
        default:
            Assert(false);
            break;
    }

    return m_disQuickLocator[nodeIdx][threadIdx];
}

int StreamProducer::findLocalChannel()
{
    for (int i = 0; i < m_connNum; i++) {
        libcommaddrinfo* addr = m_transport[i]->m_port->libcomm_addrinfo;

        /*
         * There is 4 situations when we consider consumer and producer
         * threads for redistribute:
         *             (C/P) : 1/1, 4/1, 1/4, 4/4
         * 1/1: just find the consumer of this datanode.
         * 1/4: just find the consumer of this datanode.
         * 4/1: we use roundrobin for this situation
         * 4/4: data from Nth producer thread send to Nth consumer threads
         */
        if (m_parallel_desc.consumerDop == m_parallel_desc.producerDop && u_sess->stream_cxt.producer_dop > 1) {
            if (strcmp(addr->nodename, g_instance.attr.attr_common.PGXCNodeName) == 0 &&
                addr->streamKey.consumerSmpId == (unsigned int)u_sess->stream_cxt.smp_id)
                return i;
        } else {
            if (strcmp(addr->nodename, g_instance.attr.attr_common.PGXCNodeName) == 0)
                return i;
        }
    }

    return -1;
}

/*
 * @Description: Send data by memory for local stream.
 *
 * parameter[IN] tuple: tuple to send.
 * parameter[IN] batchSrc: batch to send.
 * parameter[IN] nthChannel: the dest receiver NO.
 * parameter[IN] nthRow: the location of data in the batch.

 * @return: void
 */
void StreamProducer::sendByMemory(TupleTableSlot* tuple, VectorBatch* batchSrc, int nthChannel, int nthRow)
{
    if (m_sharedContextInit) {
        gs_memory_send(tuple, batchSrc, m_sharedContext, nthChannel, nthRow);

        bool allInValid = true;

        /*
         * Check if the connections have been closed by all the consumers when
         * the SQL is like 'limit XXX', then we should not try to send data
         * anymore, and quit now.
         */
        for (int i = 0; i < m_connNum; i++) {
            if (!m_sharedContext->is_connect_end[i][u_sess->stream_cxt.smp_id]) {
                allInValid = false;
                break;
            }
        }

        /*
         * don't set stop flag under LOCAL GATHER for MPP Recusive, we need
         * Recusive finish all sync steps, even if consumer return NULL early.
         */
        if (allInValid && !m_streamNode->is_recursive_local)
            u_sess->exec_cxt.executorStopFlag = true;
    } else {
        for (int i = 0; i < m_connNum; i++)
            gs_memory_disconnect(m_sharedContext, i);
    }
}

/*
 * @Description: When all the data has been send to consumer, give a signal to
 *				 the consumer.
 *
 * @return: void
 */
void StreamProducer::finalizeLocalStream()
{
    if (NULL == m_sharedContext)
        return;
    gs_memory_send_finish(m_sharedContext, m_connNum);
}

/*
 * @Description: Init the context for local stream through shared memory.
 *
 * @return: void
 */
void StreamProducer::initSharedContext()
{
    /* Only init when we already create a shared context for local stream. */
    if (m_sharedContext == NULL)
        return;

    if (m_sharedContext->vectorized) {
        /* Init batches. */
        for (int i = 0; i < m_connNum; i++) {
            m_sharedContext->sharedBatches[i][u_sess->stream_cxt.smp_id] =
                New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, m_desc);
        }
    } else {
        /* Init tuples. */
        for (int i = 0; i < m_connNum; i++) {
            TupleVector* TupleVec = (TupleVector*)palloc0(sizeof(TupleVector));
            TupleVec->tupleVector = (TupleTableSlot**)palloc(sizeof(TupleTableSlot*) * TupleVectorMaxSize);
            m_sharedContext->sharedTuples[i][u_sess->stream_cxt.smp_id] = TupleVec;

            for (int j = 0; j < TupleVectorMaxSize; j++) {
                TupleVec->tupleVector[j] = MakeTupleTableSlot(false);
                ExecSetSlotDescriptor(TupleVec->tupleVector[j], m_desc);
            }
        }
    }
    m_sharedContextInit = true;
}

#ifndef ENABLE_MULTIPLE_NODES
#ifdef USE_SPQ
void StreamProducer::serializeStream(VectorBatch* batch, int index)
{
    uint32 tempBufferSize = m_bitNullLen + m_bitNumericLen;
    uint8* bitNull = (uint8*)m_tempBuffer - 1;
    uint8* bitNumericFlag = (uint8*)m_tempBuffer + m_bitNullLen - 1;
 
    uint8 bitMaskNull = HIGHBIT;
    uint8 bitMaskNumeric;
    int dataLen;
    Form_pg_attribute attr;
    char* writeBuffer = NULL;
    Datum columnVal;
    int32 numericIdx = -1;
    char* string = NULL;
    errno_t rc;
 
    /* reset m_tupleBuffer and m_tempBuffer */
    resetStringInfo(&m_tupleBuffer);
    rc = memset_s(m_tempBuffer, tempBufferSize, '\0', tempBufferSize);
    securec_check(rc, "\0", "\0");
 
    m_tupleBuffer.cursor = 'B';
    /*
     * the first tempBufferSize Bits of m_tupleBuffer.data will be assigned at the end of this function
     * when null flag and numeric flag are finally determinded.
     */
    m_tupleBuffer.len = tempBufferSize;
 
    for (int i = 0; i < batch->m_cols; i++) {
        if (unlikely(bitMaskNull == HIGHBIT)) {
            /* Get null flag if i % 8 equals to 0. */
            bitNull++;
            bitMaskNull = 1;
        } else {
            /* Get next null flag for next column */
            bitMaskNull <<= 1;
        }
 
        if (NOT_NULL(batch->m_arr[i].m_flag[index])) {
            attr = &(m_desc->attrs[i]);
            /* Set null flag for index column */
            *bitNull |= bitMaskNull;
 
            columnVal = batch->m_arr[i].m_vals[index];
            /* can be stored by value directly */
            switch (m_colsType[i]) {
                case VALUE_TYPE:
                    enlargeStringInfo(&m_tupleBuffer, attr->attlen);
                    writeBuffer = m_tupleBuffer.data + m_tupleBuffer.len;
                    store_att_byval(writeBuffer, columnVal, attr->attlen);
                    m_tupleBuffer.len += attr->attlen;
                    m_tupleBuffer.data[m_tupleBuffer.len] = '\0';
                    break;
                case NUMERIC_TYPE:
                    dataLen = VARSIZE_ANY(columnVal);
                    Assert(dataLen > 0);
                    numericIdx++;
                    /*
                     * initialize numeric flag if numericIdx % 4 equals to 0.
                     * we use 01 to denote values less that 0xFF;
                     *        10 to denote values less that 0xFFFF;
                     *        11 to denote values less than 0xFFFFFFFF;
                     *        00 to denote other values.
                     */
                    if (numericIdx % 4 == 0) {
                        bitNumericFlag++;
                    }
 
                    if (!VARATT_IS_SHORT(columnVal) && NUMERIC_IS_BI64((Numeric)columnVal)) {
                        uint64 numericVal = (uint64)NUMERIC_64VALUE((Numeric)columnVal);
                        if (unlikely(numericVal <= 0xFF)) {
                            /* numeric_8_compress */
                            bitMaskNumeric = 1 << (uint32)(2 * (numericIdx % 4));  // 0x01
                            /* set numeric size flag */
                            *bitNumericFlag |= bitMaskNumeric;
                            enlargeStringInfo(&m_tupleBuffer, 2);
                            writeBuffer = m_tupleBuffer.data + m_tupleBuffer.len;
                            *(uint8*)(writeBuffer) = NUMERIC_BI_SCALE((Numeric)columnVal);
                            *(uint8*)(writeBuffer + 1) = (uint8)numericVal;
                            m_tupleBuffer.len += 2;
                            continue;
                        } else if (numericVal <= 0xFFFF) {
                            /* numeric_16_compress */
                            bitMaskNumeric = 2 << (uint32)(2 * (numericIdx % 4));  // 0x10
                            *bitNumericFlag |= bitMaskNumeric;
                            enlargeStringInfo(&m_tupleBuffer, 3);
                            writeBuffer = m_tupleBuffer.data + m_tupleBuffer.len;
                            *(uint8*)(writeBuffer) = NUMERIC_BI_SCALE((Numeric)columnVal);
                            *(uint16*)(writeBuffer + 1) = (uint16)numericVal;
                            m_tupleBuffer.len += 3;
                            continue;
                        } else if (numericVal <= 0xFFFFFFFF) {
                            /* numeric_32_compress */
                            bitMaskNumeric = 3 << (uint32)(2 * (numericIdx % 4));  // 0x11
                            *bitNumericFlag |= bitMaskNumeric;
                            enlargeStringInfo(&m_tupleBuffer, 5);
                            writeBuffer = m_tupleBuffer.data + m_tupleBuffer.len;
                            *(uint8*)(writeBuffer) = NUMERIC_BI_SCALE((Numeric)columnVal);
                            *(uint32*)(writeBuffer + 1) = (uint32)numericVal;
                            m_tupleBuffer.len += 5;
                            continue;
                        }
                    }
                    /* other numeric value lager than MAX INT32, bitMaskNumeric equals to 0x00 */
                    appendBinaryStringInfo(&m_tupleBuffer, DatumGetPointer(columnVal), dataLen);
                    break;
                case VARLENA_TYPE:
                    dataLen = VARSIZE_ANY(columnVal);
                    Assert(dataLen > 0);
                    appendBinaryStringInfo(&m_tupleBuffer, DatumGetPointer(columnVal), dataLen);
                    break;
                case CSTRING_TYPE:
                    string = VARDATA_ANY(columnVal);
                    dataLen = strlen(string) + 1;
                    appendBinaryStringInfo(&m_tupleBuffer, string, dataLen);
                    break;
                case TID_TYPE:
                    enlargeStringInfo(&m_tupleBuffer, 8);
                    writeBuffer = m_tupleBuffer.data + m_tupleBuffer.len;
                    store_att_byval(writeBuffer, columnVal, 8);
                    m_tupleBuffer.len += 8;
                    m_tupleBuffer.data[m_tupleBuffer.len] = '\0';
                    break;
                case NAME_TYPE:
                    string = ((Name)columnVal)->data;
                    dataLen = strlen(string) + 1;
                    columnVal = PointerGetDatum((char*)columnVal);
                    appendBinaryStringInfo(&m_tupleBuffer, DatumGetPointer(columnVal), dataLen);
                    break;
                case FIXED_TYPE:
                    /* extract fixed length variable. */
                    columnVal = PointerGetDatum((char*)columnVal + VARHDRSZ_SHORT);
                    appendBinaryStringInfo(&m_tupleBuffer, DatumGetPointer(columnVal), attr->attlen);
                    break;
                default:
                    Assert(false);
                    ereport(ERROR,
                        (errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                            errmodule(MOD_STREAM),
                            (errmsg("unrecognize data type %u.", m_colsType[i]))));
                    break;
            }
        }
    }
 
    /* copy null flags nd numeric flags into m_tupleBuffer */
    rc = memcpy_s(m_tupleBuffer.data, tempBufferSize, m_tempBuffer, tempBufferSize);
    securec_check(rc, "\0", "\0");
}
 
void StreamProducer::SetDest(bool is_vec_plan)
{
    switch (m_streamType) {
        case STREAM_BROADCAST:
            switch (m_parallel_desc.distriType) {
                case LOCAL_BROADCAST:
                    if (is_vec_plan)
                        m_dest = DestBatchLocalBroadCast;
                    else
                        m_dest = DestTupleLocalBroadCast;
                    break;
                case REMOTE_BROADCAST:
                    if (is_vec_plan)
                        m_dest = DestBatchBroadCast;
                    else
                        m_dest = DestTupleBroadCast;
                    break;
                default:
                    break;
            }
        case STREAM_GATHER:
        if (m_parallel_desc.distriType == REMOTE_DIRECT_DISTRIBUTE) {
            if (is_vec_plan) {
                m_dest = DestBatchRedistribute;
            }
            else{
                m_dest = DestTupleRedistribute;
            }
            setDistributeInfo();
        }
        break;
        case STREAM_REDISTRIBUTE:
            switch (m_parallel_desc.distriType) {
                case LOCAL_BROADCAST:
                    if (is_vec_plan)
                        m_dest = DestBatchLocalBroadCast;
                    else
                        m_dest = DestTupleLocalBroadCast;
 
                    break;
 
                case LOCAL_DISTRIBUTE:
                    if (is_vec_plan)
                        m_dest = DestBatchLocalRedistribute;
                    else
                        m_dest = DestTupleLocalRedistribute;
 
                    setDistributeInfo();
                    break;
 
                case LOCAL_ROUNDROBIN:
                    if (is_vec_plan)
                        m_dest = DestBatchLocalRoundRobin;
                    else
                        m_dest = DestTupleLocalRoundRobin;
                    break;
                case REMOTE_ROUNDROBIN:
                    if (is_vec_plan)
                        m_dest = DestBatchRoundRobin;
                    else
                        m_dest = DestTupleRoundRobin;
                    break;
#ifdef USE_SPQ
                case REMOTE_DML_WRITE_NODE:
                    if (!is_vec_plan)
                        m_dest = DestTupleDML;
                    break;
#endif
                case PARALLEL_NONE:
                case REMOTE_DISTRIBUTE:
                case REMOTE_SPLIT_DISTRIBUTE:
                    if (is_vec_plan)
                        m_dest = DestBatchRedistribute;
                    else
                        m_dest = DestTupleRedistribute;
 
                    setDistributeInfo();
                    break;
                default:
                    break;
            }
 
            break;
 
        case STREAM_HYBRID: {
            if (m_streamNode->distribute_keys != NIL)
                setDistributeInfo();
 
            if (is_vec_plan)
                m_dest = DestBatchHybrid;
            else
                m_dest = DestTupleHybrid;
        } break;
        default:
            break;
    }
    return;
}
 
void StreamProducer::reportNotice()
{
    m_nodeGroup->saveProducerEdata();
 
    /* for NOTICE message, report it through 0 channel only one time. */
    if (STREAM_IS_LOCAL_NODE(m_parallel_desc.distriType) && !m_isDummy) {
        if (m_sharedContextInit) {
            stream_send_message_to_consumer();
        } else {
            gs_memory_disconnect(m_sharedContext, m_nth);
        }
    } else {
        if (m_transport != NULL) {
            if (netSwitchDest(0)) {
                stream_send_message_to_consumer();
                netStatusSave(0);
            }
        }
    }
}
 
void StreamProducer::redistributeStream(VectorBatch* batch)
{
    Assert(batch != NULL);
 
    /* calc the location for each tuple in batch. */
    (this->*m_channelCalVecFun)(batch);
 
    for (int i = 0; i < batch->m_rows; i++) {
        StreamTimeSerilizeStart(t_thrd.pgxc_cxt.GlobalNetInstr);
        serializeStream(batch, i);
        StreamTimeSerilizeEnd(t_thrd.pgxc_cxt.GlobalNetInstr);
        sendByteStream(m_locator[i]);
    }
}
 
void StreamProducer::redistributeStream(TupleTableSlot* tuple, DestReceiver* self)
{
    /* calc the location for tuple. */
    (this->*m_channelCalFun)(tuple);
 
    assembleStreamMessage(tuple, self, &m_tupleBuffer);
 
    sendByteStream(m_locator[0]);
 
    /* reset the buffer. */
    resetStringInfo(&m_tupleBuffer);
}
 
void StreamProducer::broadCastStream(VectorBatch* batch)
{
    int i;
 
    Assert(m_originConsumerNodeList == NIL);
 
    assembleStreamBatchMessage(BCT_NOCOMP, batch, &m_tupleBuffer);
 
    m_broadcastSize += m_tupleBuffer.len;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize =
        Max(m_broadcastSize, t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize);
 
    if (m_broadcastSize / (1 << 20) >= WARNING_BROADCAST_SIZE) {
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning |= (1 << WLM_WARN_BROADCAST_LARGE);
    }
 
    /* When it's simple query, m_wlmParams.ptr is NULL */
    if (m_wlmParams.ptr) {
        WLMDNodeInfo* info = (WLMDNodeInfo*)m_wlmParams.ptr;
        /* Check if the broadcast size exceeds the threshold */
        if (info->geninfo.broadcastThreshold > 0 && m_broadcastSize > info->geninfo.broadcastThreshold)
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("Broadcast size exceeds the threshold: BroadcastSize=%ld, ThresholdSize=%ld, PlanId=%d",
                        m_broadcastSize,
                        info->geninfo.broadcastThreshold,
                        m_streamNode->scan.plan.plan_node_id)));
    }
 
    for (i = 0; i < m_connNum; i++)
        sendByteStream(i);
 
    resetStringInfo(&m_tupleBuffer);
}
 
void StreamProducer::broadCastStream(TupleTableSlot* tuple, DestReceiver* self)
{
    int i;
    /* assemble tuple message. */
    assembleStreamMessage(tuple, self, &m_tupleBuffer);
 
    m_broadcastSize += m_tupleBuffer.len;
    t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize =
        Max(m_broadcastSize, t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->broadcastSize);
 
    if (m_broadcastSize / (1 << 20) >= WARNING_BROADCAST_SIZE) {
        t_thrd.shemem_ptr_cxt.mySessionMemoryEntry->warning |= (1 << WLM_WARN_BROADCAST_LARGE);
    }
 
    /* When it's simple query, m_wlmParams.ptr is NULL */
    if (m_wlmParams.ptr) {
        WLMDNodeInfo* info = (WLMDNodeInfo*)m_wlmParams.ptr;
        /* Check if the broadcast size exceeds the threshold */
        if (info->geninfo.broadcastThreshold > 0 && m_broadcastSize > info->geninfo.broadcastThreshold)
            ereport(ERROR,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    errmsg("Broadcast size exceeds the threshold: BroadcastSize=%ld, ThresholdSize=%ld, PlanId=%d",
                        m_broadcastSize,
                        info->geninfo.broadcastThreshold,
                        m_streamNode->scan.plan.plan_node_id)));
    }
 
    /*
     * If original Cosumer node list is not null, we need send data to target datanode
     * with refs from its original exec_node lists.
     *
     * Only for recursive union execution
     */
    if (unlikely(m_originConsumerNodeList != NIL)) {
        for (i = 0; i < m_connNum; i++) {
            if (!list_member_int(m_originConsumerNodeList, i)) {
                continue;
            }
 
            sendByteStream(i);
        }
    } else {
        for (i = 0; i < m_connNum; i++)
            sendByteStream(i);
    }
 
    /* reset tuple buffer. */
    resetStringInfo(&m_tupleBuffer);
}
 
void StreamProducer::broadCastStreamCompress(VectorBatch* batch)
{
    int i;
 
    Assert(m_originConsumerNodeList == NIL);
 
    assembleStreamBatchMessage(BCT_LZ4, batch, &m_tupleBuffer);
 
    for (i = 0; i < m_connNum; i++)
        sendByteStream(i);
 
    resetStringInfo(&m_tupleBuffer);
}
 
void StreamProducer::roundRobinStream(TupleTableSlot* tuple, DestReceiver* self)
{
    assembleStreamMessage(tuple, self, &m_tupleBuffer);
 
    sendByteStream(m_roundRobinIdx);
 
    m_roundRobinIdx++;
    m_roundRobinIdx = m_roundRobinIdx % m_connNum;
 
    /* reset tuple buffer. */
    resetStringInfo(&m_tupleBuffer);
}

void StreamProducer::roundRobinStream(VectorBatch* batch)
{
    roundRobinBatch<BCT_LZ4>(batch);
}

#ifdef USE_SPQ
void StreamProducer::dmlStream(TupleTableSlot* tuple, DestReceiver* self)
{
    assembleStreamMessage(tuple, self, &m_tupleBuffer);
    sendByteStream(m_plan->write_node_index + m_roundRobinIdx * m_plan->num_nodes);

    m_roundRobinIdx++;
    /* only send to write node. */
    int write_dop = m_connNum / m_plan->num_nodes;
    m_roundRobinIdx = m_roundRobinIdx % write_dop;

    /* reset tuple buffer. */
    resetStringInfo(&m_tupleBuffer);
}
#endif
 
template<BatchCompressType ctype>
void StreamProducer::roundRobinBatch(VectorBatch* batch)
{
    assembleStreamBatchMessage(ctype, batch, &m_tupleBuffer);
 
    sendByteStream(m_roundRobinIdx);
 
    m_roundRobinIdx++;
    m_roundRobinIdx = m_roundRobinIdx % m_connNum;
 
    /* reset tuple buffer. */
    resetStringInfo(&m_tupleBuffer);
}
 
void StreamProducer::hybridStream(TupleTableSlot* tuple, DestReceiver* self)
{
    StreamSkew* sskew = (StreamSkew*)m_skewState;
 
    assembleStreamMessage(tuple, self, &m_tupleBuffer);
 
    switch (sskew->chooseStreamType(tuple)) {
        case STREAM_REDISTRIBUTE: {
            (this->*m_channelCalFun)(tuple);
            sendByteStream(m_locator[0]);
            break;
        }
        case STREAM_BROADCAST: {
            for (int i = 0; i < m_connNum; i++)
                sendByteStream(i);
            break;
        }
        case STREAM_ROUNDROBIN: {
            sendByteStream(m_roundRobinIdx);
 
            m_roundRobinIdx++;
            if (m_roundRobinIdx == m_connNum)
                m_roundRobinIdx = 0;
 
            break;
        }
        case STREAM_LOCAL: {
            Assert(sskew->m_localNodeId != -1);
            sendByteStream(sskew->m_localNodeId);
            break;
        }
        case STREAM_NONE: {
            break;
        }
        default:
            ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Invalid stream type for data skew.")));
    }
 
    resetStringInfo(&m_tupleBuffer);
}
 
void StreamProducer::hybridStream(VectorBatch* batch, DestReceiver* self)
{
    StreamSkew* sskew = (StreamSkew*)m_skewState;
    errno_t rc = EOK;
 
    rc = memset_s(m_skewMatch, sizeof(int) * BatchMaxSize, 0, sizeof(int) * BatchMaxSize);
    securec_check(rc, "\0", "\0");
 
    sskew->chooseVecStreamType(batch, m_skewMatch);
 
    if (m_channelCalVecFun != NULL)
        (this->*m_channelCalVecFun)(batch);
 
    for (int i = 0; i < batch->m_rows; i++) {
        m_tupleBuffer.cursor = 'B';
 
        if (m_streamNode->jitted_serialize) {
            typedef void (*serialize_func)(VectorBatch* batch, StringInfo tuplebuf, int idx);
            (void)((serialize_func)(m_streamNode->jitted_serialize))(batch, &m_tupleBuffer, i);
        } else {
            serializeStream(batch, i);
        }
 
        switch (m_skewMatch[i]) {
            case STREAM_REDISTRIBUTE: {
                sendByteStream(m_locator[i]);
                break;
            }
            case STREAM_BROADCAST: {
                for (int i = 0; i < m_connNum; i++)
                    sendByteStream(i);
                break;
            }
            case STREAM_ROUNDROBIN: {
                sendByteStream(m_roundRobinIdx);
                m_roundRobinIdx++;
                if (m_roundRobinIdx == m_connNum)
                    m_roundRobinIdx = 0;
 
                break;
            }
            case STREAM_LOCAL: {
                Assert(sskew->m_localNodeId != -1);
                sendByteStream(sskew->m_localNodeId);
                break;
            }
            case STREAM_NONE: {
                break;
            }
            default:
                ereport(
                    ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Invalid stream type for data skew.\n")));
        }
 
        resetStringInfo(&m_tupleBuffer);
    }
}
 
void StreamProducer::sendByteStream(int nthChannel)
{
    if (netSwitchDest(nthChannel)) {
        t_thrd.int_cxt.StreamConnectionLost = false;
 
#ifdef USE_ASSERT_CHECKING
        AddCheckInfo(nthChannel);
#else
        if (anls_opt_is_on(ANLS_STREAM_DATA_CHECK)) {
            AddCheckInfo(nthChannel);
        } else {
            m_transport[nthChannel]->send(m_tupleBuffer.cursor, m_tupleBuffer.data, m_tupleBuffer.len);
        }
#endif
 
        /* Stop query when cancel happend */
        if (t_thrd.int_cxt.QueryCancelPending) {
            t_thrd.int_cxt.QueryCancelPending = false;
 
            /* Must close all connection, */
            /* otherwise error message can insert into data! */
            for (int i = 0; i < m_connNum; i++)
                m_transport[i]->release();
 
            u_sess->exec_cxt.executorStopFlag = true;
        }
 
        if (t_thrd.int_cxt.StreamConnectionLost) {
            t_thrd.int_cxt.StreamConnectionLost = false;
 
            m_transport[nthChannel]->release();
 
            bool allInValid = true;
            for (int i = 0; i < m_connNum; i++) {
                if (m_transport[i]->isClosed() == false) {
                    allInValid = false;
                    break;
                }
            }
 
            if (allInValid)
                u_sess->exec_cxt.executorStopFlag = true;
        }
 
        netStatusSave(nthChannel);
    }
}
 
void StreamProducer::connectConsumer(libcomm_addrinfo** consumerAddr, int& count, int totalNum)
{
    int consumerNum = 0;
    NodeDefinition* nodesDef = NULL;
    int startCount = count;
    errno_t rc = EOK;
    int i = 0, j = 0;
    CommStreamKey key = {0};
    /* only broadcast stream support parallel send mode in libcomm */
    bool parallel_send_mode = false;
 
    if (IS_PGXC_DATANODE && ContainRecursiveUnionSubplan(m_plan)) {
        parallel_send_mode = false;
    } else {
        parallel_send_mode = (m_streamNode->type == STREAM_BROADCAST) ? true : false;
    }
    consumerNum = m_plan->num_nodes;
    nodesDef = (NodeDefinition*)palloc0(sizeof(NodeDefinition) * consumerNum);
 
    for (i = 0; i < consumerNum; i++) {
        rc = memcpy_s(&nodesDef[i], sizeof(NodeDefinition), &m_plan->nodesDefinition[i], sizeof(NodeDefinition));
        securec_check(rc, "\0", "\0");
        nodesDef[i].nodeid = i;
    }
 
    key.queryId = m_key.queryId;
    key.planNodeId = m_key.planNodeId;
    key.producerSmpId = m_key.smpIdentifier;
 
    for (i = 0; i < m_streamNode->smpDesc.consumerDop; i++) {
        /* The local stream's consumer number is 1. */
        for (j = 0; j < consumerNum; j++) {
            Assert(count < totalNum);
            int nodeNameLen = strlen(nodesDef[j].nodename.data);
            int nodehostLen = strlen(nodesDef[j].nodehost.data);
            consumerAddr[count] = (libcomm_addrinfo*)palloc0(sizeof(libcomm_addrinfo));
            consumerAddr[count]->host = (char*)palloc0(NAMEDATALEN);
            consumerAddr[count]->ctrl_port = nodesDef[j].nodectlport;
            consumerAddr[count]->listen_port = nodesDef[j].nodesctpport;
            consumerAddr[count]->nodeIdx = nodesDef[j].nodeid;
            rc = strncpy_s(consumerAddr[count]->host, NAMEDATALEN, nodesDef[j].nodehost.data, nodehostLen + 1);
            securec_check(rc, "\0", "\0");
            rc = strncpy_s(consumerAddr[count]->nodename, NAMEDATALEN, nodesDef[j].nodename.data, nodeNameLen + 1);
            securec_check(rc, "\0", "\0");
            /* use ai_next buile address info list */
            if (count > startCount)
                consumerAddr[count - 1]->addr_list_next = consumerAddr[count];
            /* set flag for parallel send mode */
            consumerAddr[count]->parallel_send_mode = parallel_send_mode;
 
            consumerAddr[count]->streamKey.queryId = m_key.queryId;
            consumerAddr[count]->streamKey.planNodeId = m_key.planNodeId;
            consumerAddr[count]->streamKey.producerSmpId = m_key.smpIdentifier;
            consumerAddr[count]->streamKey.consumerSmpId = i;
 
            count++;
        }
    }
 
    if (parallel_send_mode) {
        Assert(startCount < totalNum);
        /* set flag for the head of address info list */
        consumerAddr[startCount]->addr_list_size = count - startCount;
    }
 
    m_transport = (StreamTransport**)MemoryContextAllocZero(m_memoryCxt, m_connNum * sizeof(StreamTransport*));
    for (i = 0; i < m_connNum; i++) {
        Assert(i + startCount < totalNum);
        m_transport[i] = New(m_memoryCxt) StreamCOMM(consumerAddr[i + startCount], true);
    }
 
    pfree_ext(nodesDef);
}
#else
/*
 * @Description: Get the destnation of producer
 *
 * @param[IN] is_vec_plan:  is vector plan
 * @return: void
 */
void StreamProducer::SetDest(bool is_vec_plan)
{
    switch (m_streamType) {
        case STREAM_BROADCAST:
            if (is_vec_plan)
                m_dest = DestBatchLocalBroadCast;
            else
                m_dest = DestTupleLocalBroadCast;
            break;

        case STREAM_REDISTRIBUTE:
            switch (m_parallel_desc.distriType) {
                case LOCAL_BROADCAST:
                    if (is_vec_plan)
                        m_dest = DestBatchLocalBroadCast;
                    else
                        m_dest = DestTupleLocalBroadCast;

                    break;

                case LOCAL_DISTRIBUTE:
                    if (is_vec_plan)
                        m_dest = DestBatchLocalRedistribute;
                    else
                        m_dest = DestTupleLocalRedistribute;

                    setDistributeInfo();
                    break;

                case LOCAL_ROUNDROBIN:
                    if (is_vec_plan)
                        m_dest = DestBatchLocalRoundRobin;
                    else
                        m_dest = DestTupleLocalRoundRobin;
                    break;
 
                default:
                    break;
            }
            break; 
        default:
            break;
    }
    return;
}  

/*
 * @Description: Report notice to consumer node
 *
 * @return: void
 */
void StreamProducer::reportNotice()
{
    m_nodeGroup->saveProducerEdata();
 
    if (m_sharedContextInit) {
        stream_send_message_to_consumer();
    } else {
        gs_memory_disconnect(m_sharedContext, m_nth);
    } 
}

void StreamProducer::redistributeStream(VectorBatch* batch)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::redistributeStream(TupleTableSlot* tuple, DestReceiver* self)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::broadCastStream(VectorBatch* batch)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::broadCastStream(TupleTableSlot* tuple, DestReceiver* self)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::broadCastStreamCompress(VectorBatch* batch)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::roundRobinStream(TupleTableSlot* tuple, DestReceiver* self)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

template<BatchCompressType ctype>
void StreamProducer::roundRobinBatch(VectorBatch* batch)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::hybridStream(TupleTableSlot* tuple, DestReceiver* self)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::hybridStream(VectorBatch* batch, DestReceiver* self)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}

void StreamProducer::sendByteStream(int nthChannel)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
} 

void StreamProducer::connectConsumer(libcomm_addrinfo** consumerAddr, int& count, int totalNum)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
}
#endif
#endif

static int GetListConsumerNodeIdx(ExecBoundary* enBoundary, Const** values, int distLen)
{
    int minId, midId, maxId, hit, cmp;
    bool hasDefault = false;

    hit = -1;
    maxId = enBoundary->count - 1;
    minId = 0;
    if (enBoundary->eles[maxId]->boundary[0]->ismaxvalue) {
        hasDefault = true;
    }

    while (maxId >= minId) {
        midId = ((uint)minId + (uint)maxId) >> 1;

        cmp = partitonKeyCompare(enBoundary->eles[midId]->boundary, values, distLen);
        if (cmp == 0) {
            hit = midId;
            break;
        } else if (cmp > 0) {
            maxId = midId - 1;
        } else {
            minId = midId + 1;
        }
    }

    if (hit == -1 && hasDefault) {
        hit = enBoundary->count - 1;
    }

    if (hit != -1) {
        return enBoundary->eles[hit]->nodeIdx;
    }

    return -1;
}

static int GetRangeConsumerNodeIdx(ExecBoundary* enBoundary, Const** values, int distLen)
{
    int minId, midId, maxId, hit, cmp;
    
    maxId = enBoundary->count - 1;
    minId = 0;
    hit = -1;
    cmp = partitonKeyCompare(enBoundary->eles[maxId]->boundary, values, distLen);
    if (cmp <= 0) {
        hit = -1;
    } else {
        while (maxId > minId) {
            midId = ((uint)minId + (uint)maxId) >> 1;

            cmp = partitonKeyCompare(enBoundary->eles[midId]->boundary, values, distLen);
            if (cmp == 0) {
                hit = midId + 1;
                break;
            } else if (cmp < 0) {
                minId = midId + 1;
            } else {
                maxId = midId;
            }
        }
        if (maxId == minId) {
            hit = maxId;
        }
    }

    if (hit != -1) {
        return enBoundary->eles[hit]->nodeIdx;
    }

    return -1;
}

/*
 * used for StreamProducer NodeLocalizer Range/List redistribution.
 * report error if can't locate which datanode by values.
 */
uint2 GetTargetConsumerNodeIdx(ExecBoundary* enBoundary, Const** distValues, int distLen)
{
    int idx;

    if (enBoundary->locatorType == LOCATOR_TYPE_RANGE) {
        idx = GetRangeConsumerNodeIdx(enBoundary, distValues, distLen);
    } else if (enBoundary->locatorType == LOCATOR_TYPE_LIST) {
        idx = GetListConsumerNodeIdx(enBoundary, distValues, distLen);
    } else {
        idx = -1;
    }

    if (idx < 0 || idx > PG_UINT16_MAX) {
        ereport(ERROR, (errcode(ERRCODE_DISTRIBUTION_ERROR),
                errmsg("inserted distribution key does not map to any datanode")));
    }

    return (uint2)idx;
}
void StreamProducer::setEcontext(ExprContext* econtext)
{
    m_econtext = econtext;
}
