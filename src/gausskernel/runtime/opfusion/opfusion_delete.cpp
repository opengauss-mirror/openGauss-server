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
 * opfusion_delete.cpp
 *        Definition of delete operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_delete.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_delete.h"

#include "access/tableam.h"
#include "commands/matview.h"

DeleteFusion::DeleteFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;

    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void DeleteFusion::InitLocals(ParamListInfo params)
{
    m_local.m_tmpvals = NULL;
    m_local.m_tmpisnull = NULL;

    m_c_local.m_estate = CreateExecutorState();
    m_c_local.m_estate->es_range_table = m_global->m_planstmt->rtable;

    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
    if (m_global->m_table_type == TAM_USTORE) {
        m_local.m_reslot->tts_tupslotTableAm = TAM_USTORE;
    }
    m_local.m_values = (Datum*)palloc0(m_global->m_natts * sizeof(Datum));
    m_local.m_isnull = (bool*)palloc0(m_global->m_natts * sizeof(bool));
    initParams(params);

    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    m_local.m_optype = DELETE_FUSION;

    ModifyTable* node = (ModifyTable*)m_global->m_planstmt->planTree;
    Plan *deletePlan = (Plan *)linitial(node->plans);
    IndexScan* indexscan = (IndexScan *)JudgePlanIsPartIterator(deletePlan);
    m_local.m_scan = ScanFusion::getScanFusion((Node*)indexscan, m_global->m_planstmt,
                                               m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
}

void DeleteFusion::InitGlobals()
{
    m_global->m_reloid = getrelid(linitial_int(m_global->m_planstmt->resultRelations), m_global->m_planstmt->rtable);
    Relation rel = heap_open(m_global->m_reloid, AccessShareLock);
    m_global->m_natts = RelationGetDescr(rel)->natts;
    m_global->m_tupDesc = CreateTupleDescCopy(RelationGetDescr(rel));
    m_global->m_is_bucket_rel = RELATION_OWN_BUCKET(rel);
    m_global->m_table_type = RelationIsUstoreFormat(rel) ? TAM_USTORE : TAM_HEAP;
    m_global->m_exec_func_ptr = (OpFusionExecfuncType)&DeleteFusion::ExecDelete;
    heap_close(rel, AccessShareLock);

}

unsigned long DeleteFusion::ExecDelete(Relation rel, ResultRelInfo* resultRelInfo)
{
    bool execMatview = false;

    /*******************
     * step 1: prepare *
     *******************/

    /********************************
     * step 2: begin scan and delete*
     ********************************/
    Tuple oldtup = NULL;
    unsigned long nprocessed = 0;
    int2 bucketid = InvalidBktId;
    Relation bucket_rel = NULL;
    Relation partRel = NULL;
    Partition part = NULL;
    uint64 hash_del = 0;

    InitPartitionByScanFusion(rel, &partRel, &part, m_c_local.m_estate, m_local.m_scan);

    while ((oldtup = m_local.m_scan->getTuple()) != NULL) {
        TM_Result result;
        TM_FailureData tmfd;
        m_local.m_scan->UpdateCurrentRel(&rel);
        uint64 res_hash = 0;
        if (bucket_rel) {
            bucketCloseRelation(bucket_rel);
        }

        if (m_global->m_is_bucket_rel) {
            Assert(((HeapTuple)oldtup)->t_bucketId == 
                computeTupleBucketId(resultRelInfo->ri_RelationDesc, (HeapTuple)oldtup));
            bucketid = ((HeapTuple)oldtup)->t_bucketId;
            bucket_rel = InitBucketRelation(bucketid, rel, part);
        }

    ldelete:
	    TupleTableSlot* oldslot = NULL;
        Bitmapset *modifiedIdxAttrs = NULL;
        Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
        Relation fake_relation = (bucket_rel == NULL) ? destRel : bucket_rel;
        if (fake_relation->rd_isblockchain) {
            hash_del = get_user_tuple_hash((HeapTuple)oldtup, RelationGetDescr(fake_relation));
        }

        result = tableam_tuple_delete(fake_relation,
            &((HeapTuple)oldtup)->t_self,
            GetCurrentCommandId(true),
            InvalidSnapshot,
            GetActiveSnapshot(),
            true,
            &oldslot,
            &tmfd,
            false);

        switch (result) {
            case TM_SelfUpdated:
            case TM_SelfModified:
                if (tmfd.cmax != m_c_local.m_estate->es_output_cid)
                    ereport(ERROR,
                            (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                             errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                             errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
                /* already deleted by self; nothing to do */
                break;

            case TM_Ok:
                if (!RELATION_IS_PARTITIONED(rel)) {
                    /* Here Do the thing for Matview. */
                    execMatview = (rel != NULL && rel->rd_mlogoid != InvalidOid);
                    if (execMatview) {
                        /* judge whether need to insert into mlog-table */
                        insert_into_mlog_table(rel, rel->rd_mlogoid, NULL,
                            &((HeapTuple)oldtup)->t_self, tmfd.xmin, 'D');
                    }
                    /* done successfully */
                }
                if (fake_relation->rd_isblockchain) {
                    MemoryContext oldContext = MemoryContextSwitchTo(m_local.m_tmpContext);
                    m_local.m_ledger_hash_exist = hist_table_record_delete(fake_relation, hash_del, &res_hash);
                    m_local.m_ledger_relhash -= res_hash;
                    (void)MemoryContextSwitchTo(oldContext);
                }
                nprocessed++;
                ExecIndexTuplesState exec_index_tuples_state;
                exec_index_tuples_state.estate = m_c_local.m_estate;
                exec_index_tuples_state.targetPartRel = RELATION_IS_PARTITIONED(rel) ? partRel : NULL;
                exec_index_tuples_state.p = RELATION_IS_PARTITIONED(rel) ? part : NULL;
                exec_index_tuples_state.conflict = NULL;
                tableam_tops_exec_delete_index_tuples(oldslot, bucket_rel == NULL ? destRel : bucket_rel, NULL,
                    &((HeapTuple)oldtup)->t_self, exec_index_tuples_state, modifiedIdxAttrs);
                if (oldslot) {
                        ExecDropSingleTupleTableSlot(oldslot);
                }
                break;

            case TM_Updated: {
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }

                bool* isnullfornew = NULL;
                Datum* valuesfornew = NULL;
                Tuple copyTuple;
                m_c_local.m_estate->es_snapshot = GetActiveSnapshot();
                copyTuple = tableam_tuple_lock_updated(m_c_local.m_estate->es_output_cid,
                    fake_relation,
                    LockTupleExclusive,
                    &tmfd.ctid,
                    tmfd.xmax,
                    m_c_local.m_estate->es_snapshot);
                if (copyTuple == NULL) {
                    break;
                } 
                valuesfornew = (Datum*)palloc0(m_global->m_tupDesc->natts * sizeof(Datum));
                isnullfornew = (bool*)palloc0(m_global->m_tupDesc->natts * sizeof(bool));
                tableam_tops_deform_tuple(copyTuple, RelationGetDescr(rel), valuesfornew, isnullfornew);
                if (m_local.m_scan->EpqCheck(valuesfornew, isnullfornew) == false) {
                    pfree(valuesfornew);
                    pfree(isnullfornew);
                    break;
                }
                /* store the latest version into m_scan->m_reslot, then we can use it to delete index */
                ((HeapTuple)oldtup)->t_self = tmfd.ctid;
                oldtup = copyTuple;
                pfree(valuesfornew);
                pfree(isnullfornew);
                goto ldelete;
                
            }
            break;

            case TM_Deleted:
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                Assert(ItemPointerEquals(&((HeapTuple)oldtup)->t_self, &tmfd.ctid));
                break;

            default:
                elog(ERROR, "unrecognized heap_delete status: %u", result);
                break;
        }
    }
    (void)ExecClearTuple(m_local.m_reslot);
    if (bucket_rel) {
        bucketCloseRelation(bucket_rel);
    }
    return nprocessed;
}

bool DeleteFusion::execute(long max_rows, char *completionTag)
{
    bool success = false;
    errno_t errorno = EOK;

    /* ******************
     * step 1: prepare *
     * ***************** */
    m_local.m_scan->refreshParameter(m_local.m_outParams == NULL ? m_local.m_params : m_local.m_outParams);

    m_local.m_scan->Init(max_rows);

    Relation rel = ((m_local.m_scan->m_parentRel) == NULL ? m_local.m_scan->m_rel :
        m_local.m_scan->m_parentRel);

    ResultRelInfo *result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_c_local.m_estate->es_result_relation_info = result_rel_info;
    m_c_local.m_estate->es_output_cid = GetCurrentCommandId(true);

    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    /* *******************************
     * step 2: begin scan and delete*
     * ****************************** */
    unsigned long nprocessed = (this->*(m_global->m_exec_func_ptr))(rel, result_rel_info);
    success = true;

    if (!ScanDirectionIsNoMovement(*(m_local.m_scan->m_direction))) {
        if (max_rows == 0 || nprocessed < (unsigned long)max_rows) {
            m_local.m_isCompleted = true;
        }
        m_local.m_position += nprocessed;
    } else {
        m_local.m_isCompleted = true;
    }

    /* ***************
     * step 3: done *
     *************** */
    ExecCloseIndices(result_rel_info);
    m_local.m_isCompleted = true;
    m_local.m_scan->End(true);
    ExecDoneStepInFusion(m_c_local.m_estate);

    if (m_local.m_ledger_hash_exist && !IsConnFromApp()) {
        errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
            "DELETE %ld %lu", nprocessed, m_local.m_ledger_relhash);
    } else {
        errorno =
            snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "DELETE %ld", nprocessed);
    }
    securec_check_ss(errorno, "\0", "\0");

    return success;
}
