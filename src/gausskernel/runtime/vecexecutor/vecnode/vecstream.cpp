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
 * vecstream.cpp
 *
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/vecstream.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "executor/exec/execStream.h"
#include "pgxc/execRemote.h"
#include "nodes/nodes.h"
#include "access/printtup.h"
#include "pgxc/copyops.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolmgr.h"
#include "pgxc/locator.h"
#include "pgxc/pgxc.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include <sys/poll.h>
#include "executor/exec/execStream.h"
#include "postmaster/postmaster.h"
#include "access/transam.h"
#include "gssignal/gs_signal.h"
#include "utils/distribute_test.h"
#include "utils/guc_tables.h"
#include "utils/snapmgr.h"
#include "utils/combocid.h"
#include "vecexecutor/vecstream.h"
#include "access/hash.h"
#include "distributelayer/streamTransportComm.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"

#define attIsNull(ATT, BITS) (!((BITS)[(ATT) >> 3] & (1 << ((ATT)&0x07))))

extern void InitVecStreamMergeSort(VecStreamState* node);
extern void batchsort_getbatch(Batchsortstate* state, bool forward, VectorBatch* batch);
#ifdef PGXC
extern Batchsortstate* batchsort_begin_merge(TupleDesc tupDesc, int nkeys, AttrNumber* attNums, Oid* sortOperators,
    Oid* sortCollations, const bool* nullsFirstFlags, void* combiner, int64 workMem);
#endif

/*
 * get one vector batch for vecstream with merge sort
 */
bool StreamMergeSortGetBatch(StreamState* node)
{
    VecStreamState* snode = (VecStreamState*)node;
    batchsort_getbatch((Batchsortstate*)node->sortstate, true, snode->m_CurrentBatch);

    if (!BatchIsNull(snode->m_CurrentBatch))
        return true;
    else
        return false;
}

/*
 * init vecstream with merge sort
 */
void InitVecStreamMergeSort(VecStreamState* node)
{
    Assert(node->StreamScan == ScanStreamByLibcomm);
    Assert(node->sortstate == NULL);
    TupleTableSlot* scan_slot = node->ss.ps.ps_ResultTupleSlot;
    SimpleSort* sort = ((Stream*)(node->ss.ps.plan))->sort;

    if (node->need_fresh_data) {
        if (datanode_receive_from_logic_conn(node->conn_count, node->connections, &node->netctl, -1)) {
            int error_code = getStreamSocketError(gs_comm_strerror());
            ereport(ERROR,
                (errcode(error_code),
                    errmsg("Failed to read response from Datanodes. Detail: %s\n", gs_comm_strerror())));
        }
    }

    node->sortstate = batchsort_begin_merge(scan_slot->tts_tupleDescriptor,
        sort->numCols,
        sort->sortColIdx,
        sort->sortOperators,
        sort->sortCollations,
        sort->nullsFirst,
        node,
        u_sess->attr.attr_memory.work_mem);

    node->StreamScan = StreamMergeSortGetBatch;
}

/*
 * copy the batch from combiner to the given batch
 */
template <BatchCompressType ctype>
FORCE_INLINE static void CopyMsgToBatch(VecStreamState* vsstate, VectorBatch* batch)
{
    switch (ctype) {
        case BCT_NOCOMP:
            batch->DeserializeWithoutDecompress(vsstate->buf.msg, vsstate->buf.len);
            break;
        case BCT_LZ4:
            batch->DeserializeWithLZ4Decompress(vsstate->buf.msg, vsstate->buf.len);
            break;
        default:
            break;
    }

    vsstate->buf.len = 0;
}

/*
 * @Description: append a row to the given batch.
 *
 * @param[IN] batch:  vector batch
 * @param[IN] vs_state: deserialized message information.
 * @return: void
 */
static void append_msg_to_batch(VecStreamState* vs_state, VectorBatch* batch)
{
    char* msg = vs_state->buf.msg;
    uint32 ncols = batch->m_cols;
    int current_row = batch->m_rows;

    int bit_null_len = vs_state->bitNullLen;
    int data_len = 0;
    uint8* bit_null = (uint8*)msg;
    uint8* bit_numeric_flag = (uint8*)msg + bit_null_len - 1;
    int32 numeric_idx = -1;
    uint8 numericLen_flag = 0;
    uint8 current_numeric_len;
    char* data = msg + bit_null_len + vs_state->bitNumericLen;
    uint32* cols_type = vs_state->m_colsType;
    uint8 scale;
    uint8 val_one_byte;
    uint16 val_two_byte;
    uint32 val_four_type;
    TupleDesc desc;

    for (uint32 i = 0; i < ncols; i++) {
        ScalarVector* column = &batch->m_arr[i];
        if (!attIsNull(i, bit_null)) {
            SET_NOTNULL(column->m_flag[current_row]);
            switch (cols_type[i]) {
                case VALUE_TYPE:
                    /* value stored directly */
                    desc = vs_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
                    data_len = desc->attrs[i].attlen;
                    column->m_vals[current_row] = fetch_att(data, true, data_len);
                    break;
                case NUMERIC_TYPE:
                    numeric_idx++;
                    /* get numeric size */
                    if (numeric_idx % 4 == 0) {
                        bit_numeric_flag++;
                        numericLen_flag = bit_numeric_flag[0];
                    }
                    current_numeric_len = numericLen_flag & 0x03;
                    /* numeric less than 0xFF */
                    if (unlikely(current_numeric_len == 0x01)) {
                        data_len = 2;
                        scale = *(uint8*)data;
                        val_one_byte = *(uint8*)(data + 1);
                        column->AddShortNumericWithoutHeader((int64)val_one_byte, scale, current_row);
                        /* numeric less than 0xFFFF */
                    } else if (current_numeric_len == 0x02) {
                        data_len = 3;
                        scale = *(uint8*)data;
                        val_two_byte = *(uint16*)(data + 1);
                        column->AddShortNumericWithoutHeader((int64)val_two_byte, scale, current_row);
                        /* numeric less than 0xFFFFFFFF */
                    } else if (current_numeric_len == 0x03) {
                        data_len = 5;
                        scale = *(uint8*)data;
                        val_four_type = *(uint32*)(data + 1);
                        column->AddShortNumericWithoutHeader((int64)val_four_type, scale, current_row);
                    } else {
                        data_len = VARSIZE_ANY(data);
                        Assert(data_len > 0);
                        column->AddHeaderVar(PointerGetDatum(data), current_row);
                    }
                    numericLen_flag = numericLen_flag >> 2;
                    break;
                case VARLENA_TYPE:
                    data_len = VARSIZE_ANY(data);
                    Assert(data_len > 0);
                    column->AddHeaderVar(PointerGetDatum(data), current_row);
                    break;
                case CSTRING_TYPE:
                    data_len = strlen(data) + 1;
                    column->AddCStringVar(PointerGetDatum(data), current_row);
                    break;
                case TID_TYPE:
                    data_len = sizeof(Datum);
                    column->m_vals[current_row] = *(Datum*)data;
                    break;
                case NAME_TYPE:
                    data_len = strlen(data) + 1;
                    column->AddHeaderVar(PointerGetDatum(data), current_row);
                    break;
                case FIXED_TYPE:
                    /* fixed length value */
                    desc = vs_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
                    data_len = desc->attrs[i].attlen;
                    column->AddVar(PointerGetDatum(data), current_row);
                    break;
                default:
                    Assert(false);
                    ereport(ERROR,
                        (errmodule(MOD_STREAM),
                            (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                                errmsg("unrecognize data type %u.", cols_type[i]))));
                    break;
            }
            data += data_len;
        } else
            SET_NULL(column->m_flag[current_row]);

        column->m_rows++;
    }

    batch->m_rows++;
    vs_state->buf.len = 0;
}

/*
 * Notice:
 * the function of stream for merge sort is 'StreamGetBatchFromDatanode'
 * please both modify if need.
 */
static bool get_batch_from_conn_buffer(StreamState* node)
{
    PGXCNodeHandle** connections = NULL;
    VecStreamState* snode = (VecStreamState*)node;

    int conn_idx = node->last_conn_idx;
    connections = node->connections;
    VectorBatch* batch = snode->m_CurrentBatch;

    // handle data from all connection.
    while (conn_idx < node->conn_count) {
        int res = HandleStreamResponse(connections[conn_idx], node);

        switch (res) {
            case RESPONSE_EOF:  // try next run.
            {
                conn_idx++;
            } break;
            case RESPONSE_COMPLETE:  // finish one connection.
            {
                node->conn_count = node->conn_count - 1;

                // all finished
                if (node->conn_count == 0) {
                    node->need_fresh_data = false;
                    if (BatchIsNull(batch))
                        return false;
                    else
                        return true;
                }

                if (conn_idx < node->conn_count) {
                    connections[conn_idx] =
                        connections[node->conn_count];  // shrink for one size as one connection has finished
                }
            } break;
            case RESPONSE_DATAROW: {
                /* If we have message in the buffer, consume it */
                if (node->buf.len != 0) {
                    snode->batchForm(snode, batch);
                    node->need_fresh_data = false;
                    node->last_conn_idx = conn_idx;
                    if (batch->m_rows == BatchMaxSize || snode->redistribute == false)
                        return true;
                }
            } break;
            default:
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_DATA_EXCEPTION),
                        errmsg("Unexpected response from Datanode")));
                break;
        }
    }

    Assert(conn_idx == node->conn_count);
    node->need_fresh_data = true;
    node->last_conn_idx = 0;

    return false;
}

//
// Creates the run-time information for the stream broadcast node and redistribute node
//
VecStreamState* BuildVecStreamRuntime(Stream* node, EState* estate, int eflags)
{
    VecStreamState* stream_state = NULL;

    stream_state = makeNode(VecStreamState);
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

    // Stream runtime only set up on datanode.
    if (IS_PGXC_DATANODE)
        SetupStreamRuntime(stream_state);
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
        // Right tree should be null
        Assert(innerPlan(node) == NULL);
    }

    /* Stream runtime only set up on datanode. */
    if (IS_PGXC_DATANODE)
        SetupStreamRuntime(stream_state);

    return stream_state;
}

/*
 * @Description: Mark the data type of each column of a batch.
 *
 * @param[IN] desc: date description of batch columns
 * @param[OUT] colsType: Mark the data type of each column of a batch for redistribute stream
 * @return: void
 */
void redistributeStreamInitType(TupleDesc desc, uint32* cols_type)
{
    Form_pg_attribute attr;

    for (int i = 0; i < desc->natts; i++) {
        attr = &desc->attrs[i];

        /*
         * Mark the data type of each column.
         * If the data can be stored directly by value(length is 1B, 2B, 4B, 8B), we mark it as VALUE_TYPE;
         * For TID type, its original length is 6B, we use 8B to store it for simplification and mark it as TID_TYPE.
         * For other length date (attlen == -1(numeric or other varlena type), attlen == -2, or fixed length),
         * we mark them as NUMERIC_TYPE, VARLENA_TYPE, CSTRING_TYPE and FIXED_TYPE, respectively.
         */
        if (attr->attbyval) {
            cols_type[i] = VALUE_TYPE;
        } else if (attr->atttypid == TIDOID) {
            cols_type[i] = TID_TYPE;
        } else if (attr->atttypid == NAMEOID) {
            cols_type[i] = NAME_TYPE;
        } else if (attr->attlen == -1) {
            if (attr->atttypid == NUMERICOID)
                cols_type[i] = NUMERIC_TYPE;
            else
                cols_type[i] = VARLENA_TYPE;
        } else if (attr->attlen == -2) {
            cols_type[i] = CSTRING_TYPE;
        } else {
            cols_type[i] = FIXED_TYPE;
        }
    }
}

//
// Creates the run-time information for the stream node
//
VecStreamState* ExecInitVecStream(Stream* node, EState* estate, int eflags)
{
    VecStreamState* state = BuildVecStreamRuntime(node, estate, eflags);

    switch (node->type) {
        // gather is just like broadcast stream.
        case STREAM_GATHER:
        case STREAM_BROADCAST:
            state->redistribute = false;
            break;

        case STREAM_REDISTRIBUTE:
            if (node->smpDesc.distriType == LOCAL_ROUNDROBIN || node->smpDesc.distriType == LOCAL_BROADCAST)
                state->redistribute = false;
            else
                state->redistribute = true;
            break;

        case STREAM_HYBRID:
            state->redistribute = true;
            break;

        default:
            Assert(false);
            break;
    }

    TupleDesc res_desc = state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
    state->m_CurrentBatch = New(CurrentMemoryContext) VectorBatch(CurrentMemoryContext, res_desc);
    state->buf.msg = (char*)palloc(STREAM_BUF_INIT_SIZE);
    state->buf.len = 0;
    state->buf.size = STREAM_BUF_INIT_SIZE;
    state->isReady = false;
    state->vector_output = true;
    state->StreamScan = ScanStreamByLibcomm;

    if (STREAM_IS_LOCAL_NODE(node->smpDesc.distriType)) {
        state->StreamScan = ScanMemoryStream;
        /* local stream do not support merge sort */
        ((Stream*)(state->ss.ps.plan))->sort = NULL;
    }

    state->StreamDeserialize = get_batch_from_conn_buffer;

    if (state->redistribute) {
        state->batchForm = append_msg_to_batch;
        state->bitNumericLen = 0;
        TupleDesc desc = state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;

        for (int i = 0; i < desc->natts; i++) {
            if (desc->attrs[i].atttypid == NUMERICOID)
                state->bitNumericLen++;
        }

        state->m_colsType = (uint32*)palloc(sizeof(uint32) * (desc->natts));
        redistributeStreamInitType(desc, state->m_colsType);

        state->bitNumericLen = BITMAPLEN(2 * state->bitNumericLen);
        state->bitNullLen = BITMAPLEN(desc->natts);
    } else
        state->batchForm = CopyMsgToBatch<BCT_LZ4>;

    state->ss.ps.vectorized = true;
    state->recvDataLen = 0;

    state->type = node->type;
    return state;
}

VectorBatch* ExecVecStream(VecStreamState* node)
{
    if (unlikely(node->isReady == false)) {
        StreamPrepareRequest(node);

        /* merge sort */
        if (((Stream*)(node->ss.ps.plan))->sort != NULL) {
            InitVecStreamMergeSort(node);
        }
    }

    node->m_CurrentBatch->Reset(true);

    if (node->StreamScan(node)) {
        return node->m_CurrentBatch;
    } else {
        /* Finish receiving data from producers, can set network perf data now. */
        if (HAS_INSTR(&node->ss, false)) {
            u_sess->instr_cxt.global_instr->SetNetWork(node->ss.ps.plan->plan_node_id, *node->spill_size);
        }

        return NULL;
    }
}

void ExecEndVecStream(VecStreamState* node)
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
