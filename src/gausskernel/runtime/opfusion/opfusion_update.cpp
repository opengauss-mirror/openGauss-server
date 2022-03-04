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
 * opfusion_update.cpp
 *        Definition of update operator for bypass executor.
 *
 * IDENTIFICATION
 *        src/gausskernel/runtime/opfusion/opfusion_update.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "opfusion/opfusion_update.h"

#include "access/tableam.h"
#include "commands/matview.h"
#include "executor/node/nodeModifyTable.h"
#include "parser/parse_coerce.h"

UHeapTuple UpdateFusion::uheapModifyTuple(UHeapTuple tuple, Relation rel)
{
    UHeapTuple       newTuple;

    tableam_tops_deform_tuple(tuple, m_global->m_tupDesc, m_local.m_values, m_local.m_isnull);

    refreshTargetParameterIfNecessary();

    /*
     * create a new tuple from the values and isnull arrays
     */
    newTuple = (UHeapTuple)tableam_tops_form_tuple(m_global->m_tupDesc, m_local.m_values,
        m_local.m_isnull, UHEAP_TUPLE);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    newTuple->ctid = tuple->ctid;
    newTuple->table_oid = tuple->table_oid;
#ifdef PGXC
    newTuple->xc_node_id = tuple->xc_node_id;
#endif
    Assert (m_global->m_tupDesc->tdhasoid == false);

    return newTuple;
}

HeapTuple UpdateFusion::heapModifyTuple(HeapTuple tuple)
{
    HeapTuple new_tuple;

    tableam_tops_deform_tuple(tuple, m_global->m_tupDesc, m_local.m_values, m_local.m_isnull);

    refreshTargetParameterIfNecessary();

    /*
     * create a new tuple from the values and isnull arrays
     */
    new_tuple = (HeapTuple)tableam_tops_form_tuple(m_global->m_tupDesc, m_local.m_values, m_local.m_isnull, HEAP_TUPLE);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self, and OID
     * (if any)
     */
    new_tuple->t_data->t_ctid = tuple->t_data->t_ctid;
    new_tuple->t_self = tuple->t_self;
    new_tuple->t_tableOid = tuple->t_tableOid;
    new_tuple->t_bucketId = tuple->t_bucketId;
    HeapTupleCopyBase(new_tuple, tuple);
#ifdef PGXC
    new_tuple->t_xc_node_id = tuple->t_xc_node_id;
#endif

    Assert(m_global->m_tupDesc->tdhasoid == false);

    return new_tuple;
}

UpdateFusion::UpdateFusion(MemoryContext context, CachedPlanSource* psrc, List* plantree_list, ParamListInfo params)
    : OpFusion(context, psrc, plantree_list)
{
    MemoryContext old_context = NULL;

    if (!IsGlobal()) {
        old_context = MemoryContextSwitchTo(m_global->m_context);
        InitGlobals();
        MemoryContextSwitchTo(old_context);
    } else {
        m_c_global = ((UpdateFusion*)(psrc->opFusionObj))->m_c_global;
    }
    old_context = MemoryContextSwitchTo(m_local.m_localContext);
    InitLocals(params);
    MemoryContextSwitchTo(old_context);
}

void UpdateFusion::InitGlobals()
{
    int hash_col_num = 0;
    m_c_global = (UpdateFusionGlobalVariable*)palloc0(sizeof(UpdateFusionGlobalVariable));

    ModifyTable* node = (ModifyTable*)m_global->m_planstmt->planTree;
    Plan *updatePlan = (Plan *)linitial(node->plans);
    IndexScan* indexscan = (IndexScan *)JudgePlanIsPartIterator(updatePlan);
    m_global->m_reloid = getrelid(linitial_int(m_global->m_planstmt->resultRelations), m_global->m_planstmt->rtable);

    Relation rel = heap_open(m_global->m_reloid, AccessShareLock);
    m_global->m_table_type = RelationIsUstoreFormat(rel) ? TAM_USTORE : TAM_HEAP;
    m_global->m_exec_func_ptr = (OpFusionExecfuncType)&UpdateFusion::ExecUpdate;
    m_global->m_is_bucket_rel = RELATION_OWN_BUCKET(rel);
    m_global->m_natts = RelationGetDescr(rel)->natts;
    m_global->m_tupDesc = CreateTupleDescCopy(RelationGetDescr(rel));
    hash_col_num = rel->rd_isblockchain ? 1 : 0;
    heap_close(rel, AccessShareLock);

#ifdef USE_ASSERT_CHECKING
    if (m_global->m_is_bucket_rel) {
        /* ctid + tablebucketid */
        Assert(m_global->m_natts + 2 == list_length(indexscan->scan.plan.targetlist) + hash_col_num);
    } else {
        /* ctid */
        if (indexscan->scan.isPartTbl) {
            Assert(m_global->m_natts + 2 <= list_length(indexscan->scan.plan.targetlist) + hash_col_num);
        } else {
            Assert(m_global->m_natts + 1 <= list_length(indexscan->scan.plan.targetlist) + hash_col_num);
        }
    }
#endif
    /* init target, include param and const */
    m_c_global->m_targetParamNum = 0;
    m_c_global->m_targetParamLoc = (ParamLoc*)palloc0(m_global->m_natts * sizeof(ParamLoc));
    m_c_global->m_targetConstLoc = (ConstLoc*)palloc0(m_global->m_natts * sizeof(ConstLoc));
    m_c_global->m_targetVarLoc = (VarLoc*)palloc0(m_global->m_natts * sizeof(VarLoc));
    m_c_global->m_targetFuncNum = 0;
    m_c_global->m_targetFuncNodes = (FuncExprInfo*)palloc0(m_global->m_natts * sizeof(FuncExprInfo));
    m_c_global->m_varNum = 0;

    int i = 0;
    ListCell* lc = NULL;
    OpExpr* opexpr = NULL;
    FuncExpr* func = NULL;
    Expr* expr = NULL;

    foreach (lc, indexscan->scan.plan.targetlist) {
        /* ignore ctid + tablebucketid or ctid at last */
        if (i >= m_global->m_natts) {
            break;
        }

        TargetEntry* res = (TargetEntry*)lfirst(lc);
        expr = res->expr;

        while (IsA(expr, RelabelType)) {
            expr = ((RelabelType*)expr)->arg;
        }

        m_c_global->m_targetConstLoc[i].constLoc = -1;
        if (IsA(expr, Param)) {
            Param* param = (Param*)expr;
            m_c_global->m_targetParamLoc[m_c_global->m_targetParamNum].paramId = param->paramid;
            m_c_global->m_targetParamLoc[m_c_global->m_targetParamNum++].scanKeyIndx = i;
        } else if (IsA(expr, Const)) {
            m_c_global->m_targetConstLoc[i].constValue = ((Const*)expr)->constvalue;
            m_c_global->m_targetConstLoc[i].constIsNull = ((Const*)expr)->constisnull;
            m_c_global->m_targetConstLoc[i].constLoc = i;
        } else if (IsA(expr, Var)) {
            Var* var = (Var*)expr;
            m_c_global->m_targetVarLoc[m_c_global->m_varNum].varNo = var->varattno;
            m_c_global->m_targetVarLoc[m_c_global->m_varNum++].scanKeyIndx = i;
        } else if (IsA(expr, OpExpr)) {
            opexpr = (OpExpr*)expr;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].resno = res->resno;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].resname = res->resname;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].funcid = opexpr->opfuncid;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].args = opexpr->args;
            ++m_c_global->m_targetFuncNum;
        } else if (IsA(expr, FuncExpr)) {
            func = (FuncExpr*)expr;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].resno = res->resno;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].resname = res->resname;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].funcid = func->funcid;
            m_c_global->m_targetFuncNodes[m_c_global->m_targetFuncNum].args = func->args;
            ++m_c_global->m_targetFuncNum;
        }
        i++;
    }
    m_c_global->m_targetConstNum = i;
}

void UpdateFusion::InitLocals(ParamListInfo params)
{
    m_local.m_tmpisnull = NULL;
    m_local.m_tmpvals = NULL;
    m_c_local.m_estate = CreateExecutorState();
    m_c_local.m_estate->es_range_table = m_global->m_planstmt->rtable;

    m_local.m_reslot = MakeSingleTupleTableSlot(m_global->m_tupDesc);
    if (m_global->m_table_type == TAM_USTORE) {
        m_local.m_reslot->tts_tupslotTableAm = TAM_USTORE;
    }
    m_local.m_values = (Datum*)palloc0(m_global->m_natts * sizeof(Datum));
    m_local.m_isnull = (bool*)palloc0(m_global->m_natts * sizeof(bool));

    m_c_local.m_curVarValue = (Datum*)palloc0(m_global->m_natts * sizeof(Datum));
    m_c_local.m_curVarIsnull = (bool*)palloc0(m_global->m_natts * sizeof(bool));

    m_local.m_outParams = params;
    initParams(params);

    m_local.m_receiver = NULL;
    m_local.m_isInsideRec = true;
    m_local.m_optype = UPDATE_FUSION;

    ModifyTable *node = (ModifyTable *)m_global->m_planstmt->planTree;
    Plan *updatePlan = (Plan *)linitial(node->plans);
    IndexScan *indexscan = (IndexScan *)JudgePlanIsPartIterator(updatePlan);
    m_local.m_scan = ScanFusion::getScanFusion((Node*)indexscan, m_global->m_planstmt,
                                               m_local.m_outParams ? m_local.m_outParams : m_local.m_params);
}

void UpdateFusion::refreshTargetParameterIfNecessary()
{
    ParamListInfo parms = m_local.m_outParams != NULL ? m_local.m_outParams : m_local.m_params;
    /* save cur var value */
    for (int i = 0; i < m_global->m_tupDesc->natts; i++) {
        m_c_local.m_curVarValue[i] = m_local.m_values[i];
        m_c_local.m_curVarIsnull[i] = m_local.m_isnull[i];
    }
    if (m_c_global->m_varNum > 0) {
        for (int i = 0; i < m_c_global->m_varNum; i++) {
            m_local.m_values[m_c_global->m_targetVarLoc[i].scanKeyIndx] =
                m_c_local.m_curVarValue[m_c_global->m_targetVarLoc[i].varNo - 1];
            m_local.m_isnull[m_c_global->m_targetVarLoc[i].scanKeyIndx] =
                m_c_local.m_curVarIsnull[m_c_global->m_targetVarLoc[i].varNo - 1];
        }
    }

    /* mapping value for update from target */
    if (m_c_global->m_targetParamNum > 0) {
        Assert(m_c_global->m_targetParamNum > 0);
        for (int i = 0; i < m_c_global->m_targetParamNum; i++) {
            m_local.m_values[m_c_global->m_targetParamLoc[i].scanKeyIndx] =
                parms->params[m_c_global->m_targetParamLoc[i].paramId - 1].value;
            m_local.m_isnull[m_c_global->m_targetParamLoc[i].scanKeyIndx] =
                parms->params[m_c_global->m_targetParamLoc[i].paramId - 1].isnull;
        }
    }

    for (int i = 0; i < m_c_global->m_targetConstNum; i++) {
        if (m_c_global->m_targetConstLoc[i].constLoc >= 0) {
            m_local.m_values[m_c_global->m_targetConstLoc[i].constLoc] = m_c_global->m_targetConstLoc[i].constValue;
            m_local.m_isnull[m_c_global->m_targetConstLoc[i].constLoc] = m_c_global->m_targetConstLoc[i].constIsNull;
        }
    }
    /* calculate func result */
    for (int i = 0; i < m_c_global->m_targetFuncNum; ++i) {
        ELOG_FIELD_NAME_START(m_c_global->m_targetFuncNodes[i].resname);
        if (m_c_global->m_targetFuncNodes[i].funcid != InvalidOid) {
            bool func_isnull = false;
            m_local.m_values[m_c_global->m_targetFuncNodes[i].resno - 1] =
                CalFuncNodeVal(m_c_global->m_targetFuncNodes[i].funcid,
                               m_c_global->m_targetFuncNodes[i].args,
                               &func_isnull,
                               m_c_local.m_curVarValue,
                               m_c_local.m_curVarIsnull);
            m_local.m_isnull[m_c_global->m_targetFuncNodes[i].resno - 1] = func_isnull;
        }
        ELOG_FIELD_NAME_END;
    }
}

unsigned long UpdateFusion::ExecUpdate(Relation rel, ResultRelInfo* result_rel_info)
{
    bool execMatview = false;
    uint64 hash_del = 0;

    /*******************
     * step 1: prepare *
     *******************/
    /*********************************
     * step 2: begin scan and update *
     *********************************/
    Tuple oldtup = NULL;
    Tuple tup = NULL;
    unsigned long nprocessed = 0;
    List* recheck_indexes = NIL;
    Relation bucket_rel = NULL;
    int2 bucketid = InvalidBktId;
    Relation partRel = NULL;
    Partition part = NULL;

    InitPartitionByScanFusion(rel, &partRel, &part, m_c_local.m_estate, m_local.m_scan);

    while ((oldtup = m_local.m_scan->getTuple()) != NULL) {
        CHECK_FOR_INTERRUPTS();
        TM_Result result;
        TM_FailureData tmfd;
        uint64 res_hash = 0;
        bool modifyHist = false;

lreplace:
        t_thrd.xact_cxt.ActiveLobRelid = rel->rd_id;
        tup = tableam_tops_opfusion_modify_tuple(oldtup, m_global->m_tupDesc, m_local.m_values, m_local.m_isnull, this);
        t_thrd.xact_cxt.ActiveLobRelid = InvalidOid;
        Relation destRel = RELATION_IS_PARTITIONED(rel) ? partRel : rel;
        Relation parentRel = RELATION_IS_PARTITIONED(rel) ? rel : NULL;

        if (bucket_rel != NULL) {
            bucketCloseRelation(bucket_rel);
        }

        m_local.m_scan->UpdateCurrentRel(&rel);
        if (m_global->m_is_bucket_rel) {
            Assert(((HeapTuple)tup)->t_bucketId == 
                computeTupleBucketId(result_rel_info->ri_RelationDesc, (HeapTuple)tup));
            bucketid = ((HeapTuple)tup)->t_bucketId;
            bucket_rel = InitBucketRelation(bucketid, rel, part);
        }

        Relation ledger_dest_rel = (bucket_rel == NULL) ? destRel : bucket_rel;
        if (ledger_dest_rel->rd_isblockchain && (!RelationIsUstoreFormat(ledger_dest_rel))) {
            HeapTuple tmp_tup = (HeapTuple)tup;
            MemoryContext oldContext = MemoryContextSwitchTo(m_local.m_tmpContext);
            hash_del = get_user_tuple_hash((HeapTuple)oldtup, RelationGetDescr(ledger_dest_rel));
            tup = set_user_tuple_hash(tmp_tup, ledger_dest_rel);
            (void)MemoryContextSwitchTo(oldContext);
            tableam_tops_free_tuple(tmp_tup);
        }
        (void)ExecStoreTuple(tup, m_local.m_reslot, InvalidBuffer, false);

        /*
         * Compute stored generated columns
         */
        if (result_rel_info->ri_RelationDesc->rd_att->constr &&
            result_rel_info->ri_RelationDesc->rd_att->constr->has_generated_stored) {
            ExecComputeStoredGenerated(result_rel_info, m_c_local.m_estate, m_local.m_reslot, tup, CMD_UPDATE);
            if (tup != m_local.m_reslot->tts_tuple) {
                tableam_tops_free_tuple(tup);
                tup = m_local.m_reslot->tts_tuple;
            }
        }

        if (rel->rd_att->constr)
            ExecConstraints(result_rel_info, m_local.m_reslot, m_c_local.m_estate);

        bool update_indexes = false;
        TupleTableSlot* oldslot = NULL;
        Bitmapset *modifiedIdxAttrs = NULL;
        result = tableam_tuple_update(bucket_rel == NULL ? destRel : bucket_rel,
            bucket_rel == NULL ? parentRel : rel,
            &((HeapTuple)tup)->t_self,
            tup,
            GetCurrentCommandId(true),
            InvalidSnapshot,
            GetActiveSnapshot(),
            true,
            &oldslot,
            &tmfd,
            &update_indexes,
            &modifiedIdxAttrs,
            false);

        switch (result) {
            case TM_SelfUpdated:
            case TM_SelfModified: {
                if (tmfd.cmax != m_c_local.m_estate->es_output_cid)
                    ereport(ERROR,
                            (errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
                             errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
                             errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
                // /* already deleted by self; nothing to do */
                break;
            }
            case TM_Ok: {
                if (!RELATION_IS_PARTITIONED(rel)) {
                    /* handle with matview. */
                    execMatview = (rel != NULL && rel->rd_mlogoid != InvalidOid);
                    if (execMatview) {
                        /* judge whether need to insert into mlog-table */
                        /* 1. delete one tuple. */
                        insert_into_mlog_table(rel, rel->rd_mlogoid, NULL, &((HeapTuple)oldtup)->t_self,
                                               tmfd.xmin, 'D');
                        /* 2. insert new tuple */
                        insert_into_mlog_table(rel, rel->rd_mlogoid, (HeapTuple)tup, &(((HeapTuple)tup)->t_self),
                                               GetCurrentTransactionId(), 'I');
                    }
                    /* done successfully */
                }
                /* If an index column is updated, then we insert a new index entry. */

                if (ledger_dest_rel->rd_isblockchain) {
                    MemoryContext oldContext = MemoryContextSwitchTo(m_local.m_tmpContext);
                    modifyHist = hist_table_record_update(ledger_dest_rel, (HeapTuple)tup, hash_del, &res_hash);
                    (void)MemoryContextSwitchTo(oldContext);
                }
                if (result_rel_info->ri_NumIndices > 0 && update_indexes)
                {
                    ExecIndexTuplesState exec_index_tuples_state;
                    exec_index_tuples_state.estate = m_c_local.m_estate;
                    exec_index_tuples_state.targetPartRel = RELATION_IS_PARTITIONED(rel) ? partRel : NULL;
                    exec_index_tuples_state.p = RELATION_IS_PARTITIONED(rel) ? part : NULL;
                    exec_index_tuples_state.conflict = NULL;
                    recheck_indexes = tableam_tops_exec_update_index_tuples(m_local.m_reslot, oldslot,
                        bucket_rel == NULL ? destRel : bucket_rel,
                        NULL, tup, &((HeapTuple)oldtup)->t_self, exec_index_tuples_state, bucketid, modifiedIdxAttrs);
                }
                if (oldslot) {
                    ExecDropSingleTupleTableSlot(oldslot);
                }
                nprocessed++;
                break;
            }

            case TM_Updated: {
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                if (!RelationIsUstoreFormat(rel)) {
                    Assert(!ItemPointerEquals(&((HeapTuple)tup)->t_self, &tmfd.ctid));
                }

                bool* isnullfornew = NULL;
                Datum* valuesfornew = NULL;
                Tuple copyTuple;
                ItemPointerData tid = tmfd.ctid;
                m_c_local.m_estate->es_snapshot = GetActiveSnapshot();
                copyTuple = tableam_tuple_lock_updated(m_c_local.m_estate->es_output_cid,
                    bucket_rel == NULL ? destRel : bucket_rel,
                    LockTupleExclusive,
                    &tid,
                    tmfd.xmax,
                    m_c_local.m_estate->es_snapshot);
                if (copyTuple == NULL) {
                    pfree_ext(valuesfornew);
                    pfree_ext(isnullfornew);
                    break;
                }

                valuesfornew = (Datum*)palloc0(m_global->m_tupDesc->natts * sizeof(Datum));
                isnullfornew = (bool*)palloc0(m_global->m_tupDesc->natts * sizeof(bool));

                tableam_tops_deform_tuple(copyTuple, RelationGetDescr(rel), valuesfornew, isnullfornew);

                if (m_local.m_scan->EpqCheck(valuesfornew, isnullfornew) == false) {
                    pfree_ext(valuesfornew);
                    pfree_ext(isnullfornew);
                    break;
                }
                ((HeapTuple)oldtup)->t_self = tmfd.ctid;
                /* store the latest version into m_scan->m_reslot, then we can use it to delete index */
                (void)ExecStoreTuple(copyTuple, m_local.m_scan->m_reslot, InvalidBuffer, false);
                oldtup = copyTuple;
                pfree(valuesfornew);
                pfree(isnullfornew);

                bms_free(modifiedIdxAttrs);

                goto lreplace;

            }
            break;

            case TM_Deleted:
                if (IsolationUsesXactSnapshot()) {
                    ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                            errmsg("could not serialize access due to concurrent update")));
                }
                Assert (ItemPointerEquals(&((HeapTuple)tup)->t_self, &tmfd.ctid));
                break;

            default:
                elog(ERROR, "unrecognized update status: %u", result);
                break;
        }

        if (modifyHist) {
            m_local.m_ledger_hash_exist = true;
            m_local.m_ledger_relhash += res_hash;
        }
        bms_free(modifiedIdxAttrs);
        list_free_ext(recheck_indexes);
    }

    tableam_tops_free_tuple(tup);

    (void)ExecClearTuple(m_local.m_reslot);

    /****************
     * step 3: done *
     ****************/
    ExecCloseIndices(result_rel_info);
    m_local.m_isCompleted = true;
    m_local.m_scan->End(true);
    ExecDoneStepInFusion(m_c_local.m_estate);

    if (bucket_rel != NULL) {
        bucketCloseRelation(bucket_rel);
    }

    return nprocessed;
}

bool UpdateFusion::execute(long max_rows, char *completionTag)
{
    bool success = false;
    errno_t errorno = EOK;

    /* ******************
     * step 1: prepare *
     * ***************** */
    m_local.m_scan->refreshParameter(m_local.m_outParams == NULL ? m_local.m_params : m_local.m_outParams);
    m_local.m_scan->Init(max_rows);
    m_local.m_ledger_hash_exist = false;
    m_local.m_ledger_relhash = 0;

    Relation rel = ((m_local.m_scan->m_parentRel) == NULL ? m_local.m_scan->m_rel :
        m_local.m_scan->m_parentRel);
    ResultRelInfo *result_rel_info = makeNode(ResultRelInfo);
    InitResultRelInfo(result_rel_info, rel, 1, 0);
    m_c_local.m_estate->es_result_relation_info = result_rel_info;
    m_c_local.m_estate->es_output_cid = GetCurrentCommandId(true);

    if (result_rel_info->ri_RelationDesc->rd_rel->relhasindex) {
        ExecOpenIndices(result_rel_info, false);
    }

    /* ********************************
     * step 2: begin scan and update *
     * ******************************* */
    unsigned long nprocessed = (this->*(m_global->m_exec_func_ptr))(rel, result_rel_info);

    /* ***************
     * step 3: done *
     * ************** */
    success = true;
    if (m_local.m_ledger_hash_exist && !IsConnFromApp()) {
        errorno = snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1,
                             "UPDATE %ld %lu", nprocessed, m_local.m_ledger_relhash);
    } else {
        errorno =
            snprintf_s(completionTag, COMPLETION_TAG_BUFSIZE, COMPLETION_TAG_BUFSIZE - 1, "UPDATE %ld", nprocessed);
    }
    securec_check_ss(errorno, "\0", "\0");
    return success;
}
