/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 *
 * opfusion_selectforupdate.cpp
 * Definition of select ... for update statement for bypass executor.
 *
 * IDENTIFICATION
 * src/gausskernel/runtime/opfusion/opfusion_selectforupdate.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_selectforupdate.h"

#include "access/tableam.h"

SelectForUpdateFusion::SelectForUpdateFusion(MemoryContext context, CachedPlanSource *psrc, List *plantree_list,
    ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;
    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    } else {
        m_c_global = ((SelectForUpdateFusion *)(psrc->opFusionObj))->m_c_global;
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void SelectForUpdateFusion::InitLocals(ParamListInfo params)
{
    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
    if (m_global->m_table_type == TAM_USTORE) {
        m_local.m_reslot->tts_tupslotTableAm = TAM_USTORE;
    }
    m_c_local.m_estate = CreateExecutorState();
    m_c_local.m_estate->es_range_table = m_global->m_planstmt->rtable;
    m_local.m_values = (Datum *)palloc(m_global->m_natts * sizeof(Datum));
    m_local.m_tmpvals = (Datum *)palloc(m_global->m_natts * sizeof(Datum));
    m_local.m_isnull = (bool *)palloc(m_global->m_natts * sizeof(bool));
    m_local.m_tmpisnull = (bool *)palloc(m_global->m_natts * sizeof(bool));

    initParams(params);

    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;

    IndexScan *node = NULL;
    if (IsA(m_global->m_planstmt->planTree, Limit)) {
        node = (IndexScan *)JudgePlanIsPartIterator(m_global->m_planstmt->planTree->lefttree->lefttree);
    } else {
        node = (IndexScan *)JudgePlanIsPartIterator(m_global->m_planstmt->planTree->lefttree);
    }
    m_local.m_scan = ScanFusion::getScanFusion((Node *)node, m_global->m_planstmt,
        m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
}

void SelectForUpdateFusion::InitGlobals()
{
    m_c_global = (SelectForUpdateFusionGlobalVariable *)palloc0(sizeof(SelectForUpdateFusionGlobalVariable));

    IndexScan *node = NULL;
    m_c_global->m_limitCount = -1;
    m_c_global->m_limitOffset = -1;

    /* get limit num */
    if (IsA(m_global->m_planstmt->planTree, Limit)) {
        Limit *limit = (Limit *)m_global->m_planstmt->planTree;
        node = (IndexScan *)JudgePlanIsPartIterator(m_global->m_planstmt->planTree->lefttree->lefttree);
        if (limit->limitOffset != NULL && IsA(limit->limitOffset, Const) &&
            !((Const *)limit->limitOffset)->constisnull) {
            m_c_global->m_limitOffset = DatumGetInt64(((Const *)limit->limitOffset)->constvalue);
        }
        if (limit->limitCount != NULL && IsA(limit->limitCount, Const) && !((Const *)limit->limitCount)->constisnull) {
            m_c_global->m_limitCount = DatumGetInt64(((Const *)limit->limitCount)->constvalue);
        }
    } else {
        node = (IndexScan *)JudgePlanIsPartIterator(m_global->m_planstmt->planTree->lefttree);
    }

    List *targetList = node->scan.plan.targetlist;
    m_global->m_reloid = getrelid(node->scan.scanrelid, m_global->m_planstmt->rtable);

    Relation rel = heap_open(m_global->m_reloid, AccessShareLock);
    m_global->m_natts = RelationGetDescr(rel)->natts;
    Assert(list_length(targetList) >= 2);
    m_global->m_tupDesc = ExecCleanTypeFromTL(targetList, false, rel->rd_tam_type);
    m_global->m_is_bucket_rel = RELATION_OWN_BUCKET(rel);
    m_global->m_table_type = RelationIsUstoreFormat(rel) ? TAM_USTORE : TAM_HEAP;
    m_global->m_exec_func_ptr = (OpFusionExecfuncType)&SelectForUpdateFusion::ExecSelectForUpdate;

    heap_close(rel, AccessShareLock);

    m_global->m_attrno = (int16 *)palloc(m_global->m_natts * sizeof(int16));
    ListCell *lc = NULL;
    int cur_resno = 1;
    TargetEntry *res = NULL;
    foreach (lc, targetList) {
        res = (TargetEntry *)lfirst(lc);
        if (res->resjunk) {
            continue;
        }
        m_global->m_attrno[cur_resno - 1] = res->resorigcol;
        cur_resno++;
    }
    Assert(m_global->m_tupDesc->natts == cur_resno - 1);
}

bool SelectForUpdateFusion::execute(long max_rows, char *completionTag)
{
    MemoryContext oldContext = MemoryContextSwitchTo(m_local.m_tmpContext);
    bool success = false;

    /* ******************
     * step 1: prepare *
     * ***************** */
    int64 start_row = m_c_global->m_limitOffset >= 0 ? m_c_global->m_limitOffset : 0;
    int64 alreadyfetch = (m_local.m_position > start_row) ? (m_local.m_position - start_row) : 0;
    /* no limit get fetch size rows */
    int64 get_rows = max_rows;
    if (m_c_global->m_limitCount >= 0) {
        /* fetch size, limit */
        int64 limit_row = (m_c_global->m_limitCount - alreadyfetch > 0) ? m_c_global->m_limitCount - alreadyfetch : 0;
        get_rows = (limit_row > max_rows) ? max_rows : limit_row;
    }
    m_local.m_scan->start_row = start_row;
    m_local.m_scan->get_rows = get_rows;

    if (m_local.m_position == 0) {
        m_local.m_scan->refreshParameter(m_local.m_outParams == NULL ? m_local.m_params : m_local.m_outParams);
        m_local.m_scan->Init(max_rows);
    }

    Relation rel = ((m_local.m_scan->m_parentRel == NULL) ? m_local.m_scan->m_rel : m_local.m_scan->m_parentRel);
    ResultRelInfo *result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_c_local.m_estate->es_result_relation_info = result_rel_info;
    m_c_local.m_estate->es_output_cid = GetCurrentCommandId(true);

    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    /* *************************************
     * step 2: begin scan and update xmax *
     * ************************************ */
    setReceiver();
    unsigned long nprocessed = (this->*(m_global->m_exec_func_ptr))(rel, result_rel_info);
    success = true;

    if (!ScanDirectionIsNoMovement(*(m_local.m_scan->m_direction))) {
        if (max_rows == 0 || nprocessed < (unsigned long)max_rows) {
            m_local.m_isCompleted = true;
        }
    } else {
        m_local.m_isCompleted = true;
    }
    /* for unnamed portal, should no need to wait for next E msg */
    if (m_local.m_portalName == NULL || m_local.m_portalName[0] == '\0') {
        m_local.m_isCompleted = true;
    }

    /* ***************
     * step 3: done *
     * ************** */
    ExecCloseIndices(result_rel_info);

    m_local.m_scan->End(m_local.m_isCompleted);
    if (m_local.m_isCompleted) {
        m_local.m_position = 0;
    }
    ExecDoneStepInFusion(m_c_local.m_estate);

    if (m_local.m_isInsideRec) {
        (*m_local.m_receiver->rDestroy)(m_local.m_receiver);
    }
    errno_t errorno =
        snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "SELECT %ld", nprocessed);
    securec_check_ss(errorno, "\0", "\0");

    (void)MemoryContextSwitchTo(oldContext);
    return success;
}


unsigned long SelectForUpdateFusion::ExecSelectForUpdate(Relation rel, ResultRelInfo *result_rel_info)
{
    Tuple tuple = NULL;
    Tuple tmptup = NULL;
    unsigned long nprocessed = 0;
    int2 bucketid = InvalidBktId;
    Relation bucket_rel = NULL;
    Relation partRel = NULL;
    Partition part = NULL;

    uint64 start_row = m_local.m_scan->start_row;
    uint64 get_rows = m_local.m_scan->get_rows;

    TM_Result result;
    TM_FailureData tmfd;
    Buffer buffer;
    HeapTupleData newtuple;

    InitPartitionByScanFusion(rel, &partRel, &part, m_c_local.m_estate, m_local.m_scan);
    struct {
        HeapTupleHeaderData hdr;
        char data[MaxHeapTupleSize];
    } tbuf;

    errno_t errorno = EOK;
    errorno = memset_s(&tbuf, sizeof(tbuf), 0, sizeof(tbuf));
    securec_check(errorno, "\0", "\0");

    newtuple.t_data = &(tbuf.hdr);
    while (m_local.m_position < (long)start_row && (tuple = m_local.m_scan->getTuple()) != NULL) {
        m_local.m_position++;
    }
    if (m_local.m_position < (long)start_row) {
        Assert(tuple == NULL);
        get_rows = 0;
        m_local.m_isCompleted = true;
    }

    while (nprocessed < (unsigned long)get_rows && (tuple = m_local.m_scan->getTuple()) != NULL) {
        m_local.m_scan->UpdateCurrentRel(&rel);
        CHECK_FOR_INTERRUPTS();
        tableam_tops_deform_tuple(tuple, RelationGetDescr(rel), m_local.m_values, m_local.m_isnull);

        for (int i = 0; i < m_global->m_tupDesc->natts; i++) {
            Assert(m_global->m_attrno[i] > 0);
            m_local.m_tmpvals[i] = m_local.m_values[m_global->m_attrno[i] - 1];
            m_local.m_tmpisnull[i] = m_local.m_isnull[m_global->m_attrno[i] - 1];
        }

        tmptup = (HeapTuple)tableam_tops_form_tuple(m_global->m_tupDesc, m_local.m_tmpvals, m_local.m_tmpisnull,
            tableam_tops_get_tuple_type(rel));
        if (bucket_rel) {
            bucketCloseRelation(bucket_rel);
        }

        Assert(tmptup != NULL);

        {
            if (m_global->m_is_bucket_rel) {
                Assert(((HeapTuple)tuple)->t_bucketId ==
                    computeTupleBucketId(result_rel_info->ri_RelationDesc, (HeapTuple)tuple));
                bucketid = ((HeapTuple)tuple)->t_bucketId;
                bucket_rel = InitBucketRelation(bucketid, rel, part);
            }

            (void)ExecStoreTuple(tmptup, /* tuple to store */
                m_local.m_reslot,        /* slot to store in */
                InvalidBuffer,           /* TO DO: survey */
                false);                  /* don't pfree this pointer */

            Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
            tableam_tslot_getsomeattrs(m_local.m_reslot, m_global->m_tupDesc->natts);
            newtuple.t_self = ((HeapTuple)tuple)->t_self;
            result = tableam_tuple_lock(bucket_rel == NULL ? destRel : bucket_rel, &newtuple, &buffer,
                GetCurrentCommandId(true), LockTupleExclusive, LockWaitBlock, &tmfd,
                false, // allow_lock_self (heap implementation)
#ifdef ENABLE_MULTIPLE_NODES
                false,
#else
                true,  // follow_updates
#endif
                false, // eval
                GetActiveSnapshot(), &(((HeapTuple)tuple)->t_self), true);

            ReleaseBuffer(buffer);

            if (result == TM_SelfModified || result == TM_SelfUpdated) {
                continue;
            }

            switch (result) {
                case TM_SelfCreated:
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE), errmsg("attempted to lock invisible tuple")));
                    break;
                case TM_SelfUpdated:
                case TM_SelfModified:

                    /*
                     * The target tuple was already updated or deleted by the
                     * current command, or by a later command in the current
                     * transaction.  We *must* ignore the tuple in the former
                     * case, so as to avoid the "Halloween problem" of repeated
                     * update attempts.  In the latter case it might be sensible
                     * to fetch the updated tuple instead, but doing so would
                     * require changing heap_update and heap_delete to not
                     * complain about updating "invisible" tuples, which seems
                     * pretty scary (heap_lock_tuple will not complain, but few
                     * callers expect HeapTupleInvisible, and we're not one of
                     * them).  So for now, treat the tuple as deleted and do not
                     * process.
                     */

                    /* already deleted by self; nothing to do */
                    ExecClearTuple(m_local.m_reslot);
                    break;

                case TM_Ok:
                    /* done successfully */
                    nprocessed++;
                    m_local.m_position++;
                    (*m_local.m_receiver->receiveSlot)(m_local.m_reslot, m_local.m_receiver);
                    ExecClearTuple(m_local.m_reslot);
                    break;

                case TM_Updated: {
                    if (IsolationUsesXactSnapshot()) {
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    }
                    if (!RelationIsUstoreFormat(rel)) {
                        Assert(!ItemPointerEquals(&((HeapTuple)tuple)->t_self, &tmfd.ctid));
                    }

                    bool *isnullfornew = NULL;
                    Datum *valuesfornew = NULL;
                    Tuple copyTuple;
                    m_c_local.m_estate->es_snapshot = GetActiveSnapshot();
                    copyTuple = tableam_tuple_lock_updated(m_c_local.m_estate->es_output_cid,
                        bucket_rel == NULL ? destRel : bucket_rel, LockTupleExclusive, &tmfd.ctid, tmfd.xmax,
                        m_c_local.m_estate->es_snapshot, true);
                    if (copyTuple == NULL) {
                        break;
                    }
                    valuesfornew = (Datum *)palloc0(RelationGetDescr(rel)->natts * sizeof(Datum));
                    isnullfornew = (bool *)palloc0(RelationGetDescr(rel)->natts * sizeof(bool));

                    tableam_tops_deform_tuple(copyTuple, RelationGetDescr(rel), valuesfornew, isnullfornew);

                    if (m_local.m_scan->EpqCheck(valuesfornew, isnullfornew) == false) {
                        pfree_ext(valuesfornew);
                        pfree_ext(isnullfornew);
                        break;
                    }

                    for (int i = 0; i < m_global->m_tupDesc->natts; i++) {
                        m_local.m_tmpvals[i] = valuesfornew[m_global->m_attrno[i] - 1];
                        m_local.m_tmpisnull[i] = isnullfornew[m_global->m_attrno[i] - 1];
                    }

                    tmptup = tableam_tops_form_tuple(m_global->m_tupDesc, m_local.m_tmpvals, m_local.m_tmpisnull,
                        tableam_tops_get_tuple_type(rel));
                    Assert(tmptup != NULL);

                    (void)ExecStoreTuple(tmptup, /* tuple to store */
                        m_local.m_reslot,        /* slot to store in */
                        InvalidBuffer,           /* TO DO: survey */
                        false);                  /* don't pfree this pointer */

                    tableam_tslot_getsomeattrs(m_local.m_reslot, m_global->m_tupDesc->natts);
                    nprocessed++;
                    m_local.m_position++;
                    (*m_local.m_receiver->receiveSlot)(m_local.m_reslot, m_local.m_receiver);
                    ((HeapTuple)tuple)->t_self = tmfd.ctid;
                    tuple = copyTuple;
                    pfree(valuesfornew);
                    pfree(isnullfornew);
                    break;
                }

                case TM_Deleted:
                    if (IsolationUsesXactSnapshot()) {
                        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                    }
                    Assert(ItemPointerEquals(&((HeapTuple)tuple)->t_self, &tmfd.ctid));
                    break;
                default:
                    elog(ERROR, "unrecognized heap_lock_tuple status: %u", result);
                    break;
            }
        }
    }
    (void)ExecClearTuple(m_local.m_reslot);
    if (bucket_rel) {
        bucketCloseRelation(bucket_rel);
    }
    return nprocessed;
}

void SelectForUpdateFusion::close()
{
    if (m_local.m_isCompleted == false) {
        m_local.m_scan->End(true);
        m_local.m_isCompleted = true;
        m_local.m_position = 0;
        UnregisterSnapshot(m_local.m_snapshot);
        m_local.m_snapshot = NULL;
    }
}
