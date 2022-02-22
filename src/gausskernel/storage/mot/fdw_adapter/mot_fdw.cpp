/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
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
 * mot_fdw.cpp
 *    MOT Foreign Data Wrapper implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/fdw_adapter/mot_fdw.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <ostream>
#include <istream>
#include "global.h"
#include "mot_error.h"
#include "funcapi.h"
#include "access/reloptions.h"
#include "access/transam.h"
#include "postgres.h"

#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/date.h"
#include "utils/syscache.h"
#include "utils/partitionkey.h"
#include "catalog/heap.h"
#include "optimizer/var.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/subselect.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "mb/pg_wchar.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "parser/parsetree.h"
#include "access/sysattr.h"
#include "tcop/utility.h"
#include "postmaster/bgwriter.h"
#include "storage/lmgr.h"
#include "storage/ipc.h"

#include "mot_internal.h"
#include "storage/mot/jit_exec.h"
#include "mot_engine.h"
#include "table.h"
#include "txn.h"
#include "checkpoint_manager.h"
#include <queue>
#include "recovery_manager.h"
#include "redo_log_handler_type.h"
#include "ext_config_loader.h"
#include "utilities.h"

// allow MOT Engine logging facilities
DECLARE_LOGGER(ExternalWrapper, FDW);

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct MOTFdwOption {
    const char* m_optname;
    Oid m_optcontext; /* Oid of catalog in which option may appear */
};

/*
 * Valid options for file_fdw.
 * These options are based on the options for COPY FROM command.
 * But note that force_not_null is handled as a boolean option attached to
 * each column, not as a table option.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * fileGetOptions(), which currently doesn't bother to look at user mappings.
 */
static const struct MOTFdwOption valid_options[] = {

    {"null", ForeignTableRelationId},
    {"encoding", ForeignTableRelationId},
    {"force_not_null", AttributeRelationId},

    /* Sentinel */
    {NULL, InvalidOid}};

/*
 * SQL functions
 */
extern "C" Datum mot_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum mot_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(mot_fdw_handler);
PG_FUNCTION_INFO_V1(mot_fdw_validator);

/*
 * FDW callback routines
 */
static void MOTGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
static void MOTGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid);
static ForeignScan* MOTGetForeignPlan(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid,
    ForeignPath* best_path, List* tlist, List* scan_clauses);
static void MOTExplainForeignScan(ForeignScanState* node, ExplainState* es);
static void MOTBeginForeignScan(ForeignScanState* node, int eflags);
static TupleTableSlot* MOTIterateForeignScan(ForeignScanState* node);
static void MOTReScanForeignScan(ForeignScanState* node);
static void MOTEndForeignScan(ForeignScanState* node);
static void MOTAddForeignUpdateTargets(Query* parsetree, RangeTblEntry* targetRte, Relation targetRelation);
static int MOTAcquireSampleRowsFunc(Relation relation, int elevel, HeapTuple* rows, int targrows, double* totalrows,
    double* totaldeadrows, void* additionalData, bool estimateTableRowNum);
static bool MOTAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages,
    void* additionalData = nullptr, bool estimateTableRowNum = false);

static void MOTBeginForeignModify(
    ModifyTableState* mtstate, ResultRelInfo* resultRelInfo, List* fdwPrivate, int subplanIndex, int eflags);
static TupleTableSlot* MOTExecForeignInsert(
    EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
static TupleTableSlot* MOTExecForeignUpdate(
    EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
static TupleTableSlot* MOTExecForeignDelete(
    EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot);
static void MOTEndForeignModify(EState* estate, ResultRelInfo* resultRelInfo);

static List* MOTPlanForeignModify(PlannerInfo* root, ModifyTable* plan, ::Index resultRelation, int subplanIndex);

static void MOTXactCallback(XactEvent event, void* arg);
static void MOTSubxactCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg);

static void MOTTruncateForeignTable(TruncateStmt* stmt, Relation rel);
static void MOTVacuumForeignTable(VacuumStmt* stmt, Relation rel);
static uint64_t MOTGetForeignRelationMemSize(Oid reloid, Oid ixoid);
static MotMemoryDetail* MOTGetForeignMemSize(uint32_t* nodeCount, bool isGlobal);
static MotSessionMemoryDetail* MOTGetForeignSessionMemSize(uint32_t* sessionCount);
static void MOTNotifyForeignConfigChange();

static void MOTCheckpointCallback(CheckpointEvent checkpointEvent, uint64_t lsn, void* arg);

/*
 * Helper functions
 */
static bool IsValidOption(const char* option, Oid context);
static void MOTValidateTableDef(Node* obj);
static int MOTIsForeignRelationUpdatable(Relation rel);
static void InitMOTHandler();
MOTFdwStateSt* InitializeFdwState(void* fdwState, List** fdwExpr, uint64_t exTableId);
void* SerializeFdwState(MOTFdwStateSt* fdwState);
void ReleaseFdwState(MOTFdwStateSt* fdwState);
void CleanCursors(MOTFdwStateSt* state);
void CleanQueryStatesOnError(MOT::TxnManager* txn);

/* Query */
bool IsMOTExpr(
    RelOptInfo* baserel, MOTFdwStateSt* state, MatchIndexArr* marr, Expr* expr, Expr** result, bool setLocal);
inline bool IsNotEqualOper(OpExpr* op);

static int MOTGetFdwType()
{
    return MOT_ORC;
}

void MOTRecover()
{
    if (!MOTAdaptor::m_initialized) {
        // This is the case when StartupXLOG is called during bootstrap.
        return;
    }

    EnsureSafeThreadAccess();
    if (!MOT::MOTEngine::GetInstance()->StartRecovery()) {
        // we treat errors fatally.
        ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("MOT checkpoint recovery failed.")));
    }

    if (!g_instance.attr.attr_common.enable_thread_pool) {
        MOT::SessionContext* ctx = MOT_GET_CURRENT_SESSION_CONTEXT();
        on_proc_exit(MOTAdaptor::DestroyTxn, PointerGetDatum(ctx));
        MOT_LOG_INFO("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
    }
}

void MOTRecoveryDone()
{
    if (!MOTAdaptor::m_initialized) {
        // This is the case when StartupXLOG is called during bootstrap.
        return;
    }

    EnsureSafeThreadAccess();
    if (!MOT::MOTEngine::GetInstance()->EndRecovery()) {

        ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("MOT recovery failed.")));
    }
}

/*
 * The following two functions should be used if the MOT redo recovery is done in a separate thread.
 */
void MOTBeginRedoRecovery()
{
    if (!MOTAdaptor::m_initialized) {
        return;
    }

    EnsureSafeThreadAccess();
    if (!MOT::MOTEngine::GetInstance()->CreateRecoverySessionContext()) {
        // we treat errors fatally.
        ereport(FATAL, (errmsg("MOTBeginRedoRecovery: failed to create session context.")));
    }

    if (!g_instance.attr.attr_common.enable_thread_pool) {
        MOT::SessionContext* ctx = MOT_GET_CURRENT_SESSION_CONTEXT();
        on_proc_exit(MOTAdaptor::DestroyTxn, PointerGetDatum(ctx));
        MOT_LOG_INFO("Registered current thread for proc-exit callback for thread %p", (void*)pthread_self());
    }
}

void MOTEndRedoRecovery()
{
    if (!MOTAdaptor::m_initialized) {
        return;
    }

    EnsureSafeThreadAccess();
    MOT::MOTEngine::GetInstance()->DestroyRecoverySessionContext();
    knl_thread_mot_init();  // reset all thread locals
}

/*
 * This function should be called upon startup in order to enable xlog replay into
 * mot. We call it in mot_fdw_handler just in case
 */
void InitMOT()
{
    if (MOTAdaptor::m_initialized) {
        // MOT is already initialized, probably it's primary switch-over to standby.
        return;
    }

    InitMOTHandler();
    JitExec::JitInitialize();
}

/**
 * Shutdown the engine
 */
void TermMOT()
{
    if (!MOTAdaptor::m_initialized) {
        return;
    }

    JitExec::JitDestroy();
    MOTAdaptor::Destroy();
}

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum mot_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine* fdwroutine = makeNode(FdwRoutine);
    fdwroutine->AddForeignUpdateTargets = MOTAddForeignUpdateTargets;
    fdwroutine->GetForeignRelSize = MOTGetForeignRelSize;
    fdwroutine->GetForeignPaths = MOTGetForeignPaths;
    fdwroutine->GetForeignPlan = MOTGetForeignPlan;
    fdwroutine->PlanForeignModify = MOTPlanForeignModify;
    fdwroutine->ExplainForeignScan = MOTExplainForeignScan;
    fdwroutine->BeginForeignScan = MOTBeginForeignScan;
    fdwroutine->IterateForeignScan = MOTIterateForeignScan;
    fdwroutine->ReScanForeignScan = MOTReScanForeignScan;
    fdwroutine->EndForeignScan = MOTEndForeignScan;
    fdwroutine->AnalyzeForeignTable = MOTAnalyzeForeignTable;
    fdwroutine->AcquireSampleRows = MOTAcquireSampleRowsFunc;
    fdwroutine->ValidateTableDef = MOTValidateTableDef;
    fdwroutine->PartitionTblProcess = NULL;
    fdwroutine->BuildRuntimePredicate = NULL;
    fdwroutine->BeginForeignModify = MOTBeginForeignModify;
    fdwroutine->ExecForeignInsert = MOTExecForeignInsert;
    fdwroutine->ExecForeignUpdate = MOTExecForeignUpdate;
    fdwroutine->ExecForeignDelete = MOTExecForeignDelete;
    fdwroutine->EndForeignModify = MOTEndForeignModify;
    fdwroutine->IsForeignRelUpdatable = MOTIsForeignRelationUpdatable;
    fdwroutine->GetFdwType = MOTGetFdwType;
    fdwroutine->TruncateForeignTable = MOTTruncateForeignTable;
    fdwroutine->VacuumForeignTable = MOTVacuumForeignTable;
    fdwroutine->GetForeignRelationMemSize = MOTGetForeignRelationMemSize;
    fdwroutine->GetForeignMemSize = MOTGetForeignMemSize;
    fdwroutine->GetForeignSessionMemSize = MOTGetForeignSessionMemSize;
    fdwroutine->NotifyForeignConfigChange = MOTNotifyForeignConfigChange;

    if (!u_sess->mot_cxt.callbacks_set) {
        GetCurrentTransactionIdIfAny();
        RegisterXactCallback(MOTXactCallback, NULL);
        RegisterSubXactCallback(MOTSubxactCallback, NULL);
        u_sess->mot_cxt.callbacks_set = true;
    }

    PG_TRY();
    {
        MOTAdaptor::InitTxnManager(__FUNCTION__);
    }
    PG_CATCH();
    {
        elog(LOG, "Failed to init MOT transaction manager in FDW initializer");
    }
    PG_END_TRY();
    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum mot_fdw_validator(PG_FUNCTION_ARGS)
{
    List* optionsList = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);
    List* otherOptions = NIL;
    ListCell* cell = nullptr;

    /*
     * Note that the valid_options[] array disallows setting filename at any
     * options level other than foreign table --- otherwise there'd still be a
     * security hole.
     */
    foreach (cell, optionsList) {
        DefElem* def = (DefElem*)lfirst(cell);

        if (!IsValidOption(def->defname, catalog)) {
            const struct MOTFdwOption* opt = nullptr;
            StringInfoData buf;

            /*
             * Unknown option specified, complain about it. Provide a hint
             * with list of valid options for the object.
             */
            initStringInfo(&buf);
            for (opt = valid_options; opt->m_optname; opt++) {
                if (catalog == opt->m_optcontext)
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "", opt->m_optname);
            }

            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("invalid option \"%s\"", def->defname),
                    buf.len > 0 ? errhint("Valid options in this context are: %s", buf.data)
                                : errhint("There are no valid options in this context.")));
        }
    }

    /*
     * Now apply the core COPY code's validation logic for more checks.
     */
    ProcessCopyOptions(NULL, true, otherOptions);

    PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool IsValidOption(const char* option, Oid context)
{
    const struct MOTFdwOption* opt;

    for (opt = valid_options; opt->m_optname; opt++) {
        if (context == opt->m_optcontext && strcmp(opt->m_optname, option) == 0)
            return true;
    }
    return false;
}

/*
 * Check if there is any memory management module error.
 * If there is any, abort the whole transaction.
 */
static void MemoryEreportError()
{
    int result = MOT::GetLastError();
    if (result == MOT_ERROR_INVALID_MEMORY_SIZE) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid memory alloc request size.")));
    } else if (result == MOT_ERROR_OOM) {
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_LOGICAL_MEMORY), errmsg("Memory is temporarily unavailable.")));
    }
}

/*
 *
 */
static void MOTGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    MOTFdwStateSt* planstate = (MOTFdwStateSt*)palloc0(sizeof(MOTFdwStateSt));
    ForeignTable* ftable = GetForeignTable(foreigntableid);
    MOT::TxnManager* currTxn = GetSafeTxn(__FUNCTION__);
    Bitmapset* attrs = nullptr;
    ListCell* lc = nullptr;
    bool needWholeRow = false;
    TupleDesc desc;
    Relation rel = RelationIdGetRelation(ftable->relid);

    planstate->m_table = currTxn->GetTableByExternalId(RelationGetRelid(rel));
    if (planstate->m_table == nullptr) {
        abortParentTransactionParamsNoDetail(
            ERRCODE_UNDEFINED_TABLE, MOT_TABLE_NOTFOUND, (char*)RelationGetRelationName(rel));
        return;
    }
    baserel->fdw_private = planstate;
    planstate->m_hasForUpdate = root->parse->hasForUpdate;
    planstate->m_cmdOper = root->parse->commandType;
    planstate->m_foreignTableId = foreigntableid;
    desc = RelationGetDescr(rel);
    planstate->m_numAttrs = RelationGetNumberOfAttributes(rel);
    int len = BITMAP_GETLEN(planstate->m_numAttrs);
    planstate->m_attrsUsed = (uint8_t*)palloc0(len);
    planstate->m_attrsModified = (uint8_t*)palloc0(len);
    needWholeRow = rel->trigdesc && rel->trigdesc->trig_insert_after_row;

    foreach (lc, baserel->baserestrictinfo) {
        RestrictInfo* ri = (RestrictInfo*)lfirst(lc);

        if (!needWholeRow)
            pull_varattnos((Node*)ri->clause, baserel->relid, &attrs);
    }

    if (needWholeRow) {
        for (int i = 0; i < desc->natts; i++) {
            if (!desc->attrs[i]->attisdropped) {
                BITMAP_SET(planstate->m_attrsUsed, (desc->attrs[i]->attnum - 1));
            }
        }
    } else {
        /* Pull "var" clauses to build an appropriate target list */
        pull_varattnos((Node*)baserel->reltargetlist, baserel->relid, &attrs);
        if (attrs != NULL) {
            bool all = bms_is_member(-FirstLowInvalidHeapAttributeNumber, attrs);
            for (int i = 0; i < planstate->m_numAttrs; i++) {
                if (all || bms_is_member(desc->attrs[i]->attnum - FirstLowInvalidHeapAttributeNumber, attrs)) {
                    BITMAP_SET(planstate->m_attrsUsed, (desc->attrs[i]->attnum - 1));
                }
            }
        }
    }

    baserel->rows = planstate->m_table->GetRowCount();
    baserel->tuples = planstate->m_table->GetRowCount();
    if (baserel->rows == 0)
        baserel->rows = baserel->tuples = 100000;
    planstate->m_startupCost = 0.1;
    planstate->m_totalCost = baserel->rows * planstate->m_startupCost;

    RelationClose(rel);
}

static bool IsOrderingApplicable(PathKey* pathKey, RelOptInfo* rel, MOT::Index* ix, OrderSt* ord)
{
    bool res = false;

    if (ord->m_order == SORTDIR_ENUM::SORTDIR_NONE)
        ord->m_order = SORT_STRATEGY(pathKey->pk_strategy);
    else if (ord->m_order != SORT_STRATEGY(pathKey->pk_strategy))
        return res;

    do {
        const int16_t* cols = ix->GetColumnKeyFields();
        int16_t numKeyCols = ix->GetNumFields();
        ListCell* lcEm = nullptr;

        foreach (lcEm, pathKey->pk_eclass->ec_members) {
            EquivalenceMember* em = (EquivalenceMember*)lfirst(lcEm);

            if (bms_equal(em->em_relids, rel->relids)) {
                if (IsA(em->em_expr, Var)) {
                    Var* v = (Var*)(em->em_expr);
                    int i = 0;
                    for (; i < numKeyCols; i++) {
                        if (cols[i] == v->varattno) {
                            if (ord->m_lastMatch != -1 && ((ord->m_lastMatch + 1) != i)) {
                                res = false;
                                break;
                            }
                            ord->m_cols[i] = 1;
                            ord->m_lastMatch = i;
                            res = true;
                            break;
                        }
                    }

                    // order column is not part of the index
                    // the index ordering can not be applied
                    if (i == numKeyCols || res == false) {
                        res = false;
                        break;
                    }
                }
            }
        }
    } while (0);

    return res;
}

/*
 *
 */
static void MOTGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid)
{
    MOTFdwStateSt* planstate = (MOTFdwStateSt*)baserel->fdw_private;
    List* usablePathkeys = NIL;
    List* bestClause = nullptr;
    Path* bestPath = nullptr;
    MatchIndexArr marr;
    ListCell* lc = nullptr;
    MatchIndex* best = nullptr;
    Path* fpReg = nullptr;
    Path* fpIx = nullptr;
    bool hasRegularPath = false;

    planstate->m_order = SORTDIR_ENUM::SORTDIR_ASC;
    // first create regular path based on relation restrictions
    foreach (lc, baserel->baserestrictinfo) {
        RestrictInfo* ri = (RestrictInfo*)lfirst(lc);

        if (!IsMOTExpr(baserel, planstate, &marr, ri->clause, nullptr, true)) {
            planstate->m_localConds = lappend(planstate->m_localConds, ri->clause);
        }
    }

    // get best index
    best = MOTAdaptor::GetBestMatchIndex(planstate, &marr, list_length(baserel->baserestrictinfo));
    if (best != nullptr) {
        OrderSt ord;
        ord.init();
        double ntuples = best->m_cost;
        ntuples = ntuples * clauselist_selectivity(root, bestClause, 0, JOIN_INNER, nullptr);
        ntuples = clamp_row_est(ntuples);
        baserel->rows = baserel->tuples = ntuples;
        planstate->m_startupCost = 0.001;
        planstate->m_totalCost = best->m_cost;
        planstate->m_bestIx = best;

        foreach (lc, root->query_pathkeys) {
            PathKey* pathkey = (PathKey*)lfirst(lc);

            if (!pathkey->pk_eclass->ec_has_volatile && IsOrderingApplicable(pathkey, baserel, best->m_ix, &ord)) {
                usablePathkeys = lappend(usablePathkeys, pathkey);
            }
        }

        if (!best->CanApplyOrdering(ord.m_cols)) {
            list_free(usablePathkeys);
            usablePathkeys = nullptr;
        } else if (!best->AdjustForOrdering((ord.m_order == SORTDIR_ENUM::SORTDIR_DESC))) {
            list_free(usablePathkeys);
            usablePathkeys = nullptr;
        }
        best = nullptr;
    } else if (list_length(root->query_pathkeys) > 0) {
        OrderSt ord;
        ord.init();
        MOT::Index* ix = planstate->m_table->GetPrimaryIndex();
        List* keys;

        if (root->query_level == 1)
            keys = root->query_pathkeys;
        else
            keys = root->sort_pathkeys;

        foreach (lc, keys) {
            PathKey* pathkey = (PathKey*)lfirst(lc);

            if (!pathkey->pk_eclass->ec_has_volatile && IsOrderingApplicable(pathkey, baserel, ix, &ord)) {
                usablePathkeys = lappend(usablePathkeys, pathkey);
            }
        }

        if (list_length(usablePathkeys) > 0) {
            if (ord.m_cols[0] != 0)
                planstate->m_order = ord.m_order;
            else {
                list_free(usablePathkeys);
                usablePathkeys = nullptr;
            }
        } else
            planstate->m_order = SORTDIR_ENUM::SORTDIR_ASC;
    }

    fpReg = (Path*)create_foreignscan_path(root,
        baserel,
        planstate->m_startupCost,
        planstate->m_totalCost,
        usablePathkeys,
        nullptr,  /* no outer rel either */
        nullptr,  // private data will be assigned later
        0);

    foreach (lc, baserel->pathlist) {
        Path* path = (Path*)lfirst(lc);
        if (IsA(path, IndexPath) && path->param_info == nullptr) {
            hasRegularPath = true;
            break;
        }
    }
    if (!hasRegularPath)
        add_path(root, baserel, fpReg);
    set_cheapest(baserel);

    if (!IS_PGXC_COORDINATOR && list_length(baserel->cheapest_parameterized_paths) > 0) {
        foreach (lc, baserel->cheapest_parameterized_paths) {
            bestPath = (Path*)lfirst(lc);
            if (IsA(bestPath, IndexPath) && bestPath->param_info) {
                IndexPath* ip = (IndexPath*)bestPath;
                bestClause = ip->indexclauses;
                break;
            }
        }
        usablePathkeys = nullptr;
    }

    if (bestClause != nullptr) {
        marr.Clear();

        foreach (lc, bestClause) {
            RestrictInfo* ri = (RestrictInfo*)lfirst(lc);

            // In case we use index params DO NOT add it to envelope filter.
            (void)IsMOTExpr(baserel, planstate, &marr, ri->clause, nullptr, false);
        }

        best = MOTAdaptor::GetBestMatchIndex(planstate, &marr, list_length(bestClause), false);
        if (best != nullptr) {
            OrderSt ord;
            ord.init();
            double ntuples = best->m_cost;
            ntuples = ntuples * clauselist_selectivity(root, bestClause, 0, JOIN_INNER, nullptr);
            ntuples = clamp_row_est(ntuples);
            baserel->rows = baserel->tuples = ntuples;
            planstate->m_paramBestIx = best;
            planstate->m_startupCost = 0.001;
            planstate->m_totalCost = best->m_cost;

            foreach (lc, root->query_pathkeys) {
                PathKey* pathkey = (PathKey*)lfirst(lc);
                if (!pathkey->pk_eclass->ec_has_volatile && IsOrderingApplicable(pathkey, baserel, best->m_ix, &ord)) {
                    usablePathkeys = lappend(usablePathkeys, pathkey);
                }
            }

            if (!best->CanApplyOrdering(ord.m_cols)) {
                list_free(usablePathkeys);
                usablePathkeys = nullptr;
            } else if (!best->AdjustForOrdering((ord.m_order == SORTDIR_ENUM::SORTDIR_DESC))) {
                list_free(usablePathkeys);
                usablePathkeys = nullptr;
            }

            fpIx = (Path*)create_foreignscan_path(root,
                baserel,
                planstate->m_startupCost,
                planstate->m_totalCost,
                usablePathkeys,
                nullptr,  /* no outer rel either */
                nullptr,  // private data will be assigned later
                0);

            fpIx->param_info = bestPath->param_info;
        }
    }

    List* newPath = nullptr;
    List* origPath = baserel->pathlist;
    // disable index path
    foreach (lc, baserel->pathlist) {
        Path* path = (Path*)lfirst(lc);
        if (IsA(path, ForeignPath))
            newPath = lappend(newPath, path);
        else
            pfree(path);
    }

    list_free(origPath);
    baserel->pathlist = newPath;
    if (hasRegularPath)
        add_path(root, baserel, fpReg);
    if (fpIx != nullptr)
        add_path(root, baserel, fpIx);
    set_cheapest(baserel);
}

/*
 *
 */
static ForeignScan* MOTGetForeignPlan(
    PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid, ForeignPath* best_path, List* tlist, List* scan_clauses)
{
    ListCell* lc = nullptr;
    ::Index scanRelid = baserel->relid;
    MOTFdwStateSt* planstate = (MOTFdwStateSt*)baserel->fdw_private;
    List* tmpLocal = nullptr;
    List* remote = nullptr;

    if (best_path->path.param_info && planstate->m_paramBestIx) {
        if (planstate->m_bestIx != nullptr) {
            planstate->m_bestIx->Clean(planstate);
            pfree(planstate->m_bestIx);
        }
        planstate->m_bestIx = planstate->m_paramBestIx;
        planstate->m_paramBestIx = nullptr;
    }

    if (planstate->m_bestIx != nullptr) {
        planstate->m_numExpr = list_length(planstate->m_bestIx->m_remoteConds);
        remote = list_concat(planstate->m_bestIx->m_remoteConds, planstate->m_bestIx->m_remoteCondsOrig);

        if (planstate->m_bestIx->m_remoteCondsOrig) {
            pfree(planstate->m_bestIx->m_remoteCondsOrig);
            planstate->m_bestIx->m_remoteCondsOrig = nullptr;
        }
    } else {
        planstate->m_numExpr = 0;
    }
    baserel->fdw_private = nullptr;
    tmpLocal = planstate->m_localConds;
    planstate->m_localConds = nullptr;

    foreach (lc, scan_clauses) {
        RestrictInfo* ri = (RestrictInfo*)lfirst(lc);

        // add OR conditions which where not handled by previous functions
        if (ri->orclause != nullptr)
            planstate->m_localConds = lappend(planstate->m_localConds, ri->clause);
        else if (IsA(ri->clause, BoolExpr)) {
            BoolExpr* e = (BoolExpr*)ri->clause;

            if (e->boolop == NOT_EXPR)
                planstate->m_localConds = lappend(planstate->m_localConds, ri->clause);
        } else if (IsA(ri->clause, OpExpr)) {
            OpExpr* e = (OpExpr*)ri->clause;

            if (IsNotEqualOper(e))
                planstate->m_localConds = lappend(planstate->m_localConds, ri->clause);
            else if (!list_member(remote, e))
                planstate->m_localConds = lappend(planstate->m_localConds, ri->clause);
        }
    }

    foreach (lc, tmpLocal) {
        Expr* e = (Expr*)lfirst(lc);
        if (!list_member(planstate->m_localConds, e))
            planstate->m_localConds = lappend(planstate->m_localConds, e);
    }

    if (tmpLocal != nullptr)
        list_free(tmpLocal);

    List* quals = planstate->m_localConds;
    return make_foreignscan(tlist,
        quals,
        scanRelid,
        remote, /* no expressions to evaluate */
        (List*)SerializeFdwState(planstate)
#if PG_VERSION_NUM >= 90500
            ,
        nullptr,
        nullptr, /* All quals are meant to be rechecked */
        nullptr
#endif
    );
}

/*
 *
 *		Produce extra output for EXPLAIN
 */
static void MOTExplainForeignScan(ForeignScanState* node, ExplainState* es)
{
    MOTFdwStateSt* festate = nullptr;
    bool isLocal = false;
    ForeignScan* fscan = (ForeignScan*)node->ss.ps.plan;

    if (node->fdw_state != nullptr)
        festate = (MOTFdwStateSt*)node->fdw_state;
    else {
        festate =
            InitializeFdwState(fscan->fdw_private, &fscan->fdw_exprs, RelationGetRelid(node->ss.ss_currentRelation));
        isLocal = true;
    }

    ExplainPropertyInteger("->  Memory Engine returned rows", festate->m_rowsFound, es);

    if (festate->m_bestIx != nullptr) {
        Node* qual = nullptr;
        List* context = nullptr;
        char* exprstr = nullptr;

        // index name
        appendStringInfoSpaces(es->str, es->indent);
        ExplainPropertyText("->  Index Scan on", festate->m_bestIx->m_ix->GetName().c_str(), es);
        es->indent += 2;

        // details for index
        appendStringInfoSpaces(es->str, es->indent);

        qual = (Node*)make_ands_explicit(festate->m_remoteCondsOrig);

        /* Set up deparsing context */
        context = deparse_context_for_planstate((Node*)&(node->ss.ps), NULL, es->rtable);

        /* Deparse the expression */
        exprstr = deparse_expression(qual, context, true, false);

        if (t_thrd.explain_cxt.explain_perf_mode != EXPLAIN_NORMAL && es->planinfo->m_detailInfo) {
            es->planinfo->m_detailInfo->set_plan_name<true, true>();
            appendStringInfo(es->planinfo->m_detailInfo->info_str, "%s: %s\n", "Index Cond", exprstr);
        }

        /* And add to es->str */
        ExplainPropertyText("Index Cond", exprstr, es);
        es->indent += 2;
    }

    if (isLocal) {
        ReleaseFdwState(festate);
    }
}

/*
 *
 */
static void MOTBeginForeignScan(ForeignScanState* node, int eflags)
{
    ListCell* t = NULL;
    MOTFdwStateSt* festate;
    ForeignScan* fscan = (ForeignScan*)node->ss.ps.plan;

    node->ss.is_scan_end = false;
    festate = InitializeFdwState(fscan->fdw_private, &fscan->fdw_exprs, RelationGetRelid(node->ss.ss_currentRelation));
    MOTAdaptor::GetCmdOper(festate);
    festate->m_txnId = GetCurrentTransactionIdIfAny();
    festate->m_currTxn = GetSafeTxn(__FUNCTION__);
    festate->m_currTxn->IncStmtCount();
    festate->m_currTxn->m_queryState[(uint64_t)festate] = (uint64_t)festate;
    festate->m_table = festate->m_currTxn->GetTableByExternalId(RelationGetRelid(node->ss.ss_currentRelation));
    node->fdw_state = festate;
    if (node->ss.ps.state->es_result_relation_info &&
        RelationGetRelid(node->ss.ps.state->es_result_relation_info->ri_RelationDesc) ==
            RelationGetRelid(node->ss.ss_currentRelation))
        node->ss.ps.state->es_result_relation_info->ri_FdwState = festate;
    festate->m_currTxn->SetTxnIsoLevel(u_sess->utils_cxt.XactIsoLevel);

    foreach (t, node->ss.ps.plan->targetlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(t);
        Var* v = (Var*)tle->expr;
        if (v->varattno == SelfItemPointerAttributeNumber && v->vartype == TIDOID) {
            festate->m_ctidNum = tle->resno;
            break;
        }
    }
}

static void MOTBeginForeignModify(
    ModifyTableState* mtstate, ResultRelInfo* resultRelInfo, List* fdwPrivate, int subplanIndex, int eflags)
{
    MOTFdwStateSt* festate = nullptr;

    if (fdwPrivate != nullptr && resultRelInfo->ri_FdwState == nullptr) {
        isMemoryLimitReached();
        festate = InitializeFdwState(fdwPrivate, nullptr, RelationGetRelid(resultRelInfo->ri_RelationDesc));
        festate->m_allocInScan = false;
        festate->m_txnId = GetCurrentTransactionIdIfAny();
        festate->m_currTxn = GetSafeTxn(__FUNCTION__);
        festate->m_currTxn->m_queryState[(uint64_t)festate] = (uint64_t)festate;
        festate->m_table = festate->m_currTxn->GetTableByExternalId(RelationGetRelid(resultRelInfo->ri_RelationDesc));
        resultRelInfo->ri_FdwState = festate;
    } else {
        festate = (MOTFdwStateSt*)resultRelInfo->ri_FdwState;
        if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached()) {
            CleanQueryStatesOnError(festate->m_currTxn);
        }
        switch (mtstate->operation) {
            case CMD_INSERT:
            case CMD_UPDATE:
                isMemoryLimitReached();
                break;
            default:
                break;
        }
        // bring all attributes
        int len = BITMAP_GETLEN(festate->m_numAttrs);
        if (fdwPrivate != nullptr) {
            ListCell* cell = list_head(fdwPrivate);
            BitmapDeSerialize(festate->m_attrsModified, len, &cell);

            for (int i = 0; i < festate->m_numAttrs; i++) {
                if (BITMAP_GET(festate->m_attrsModified, i)) {
                    BITMAP_SET(festate->m_attrsUsed, i);
                }
            }
        } else {
            errno_t erc = memset_s(festate->m_attrsUsed, len, 0xff, len);
            securec_check(erc, "\0", "\0");
            erc = memset_s(festate->m_attrsModified, len, 0xff, len);
            securec_check(erc, "\0", "\0");
        }
    }
    // Update FDW operation
    festate->m_ctidNum =
        ExecFindJunkAttributeInTlist(mtstate->mt_plans[subplanIndex]->plan->targetlist, MOT_REC_TID_NAME);
    festate->m_cmdOper = mtstate->operation;
    MOTAdaptor::GetCmdOper(festate);
    festate->m_currTxn->SetTxnIsoLevel(u_sess->utils_cxt.XactIsoLevel);
}

static TupleTableSlot* IterateForeignScanStopAtFirst(
    ForeignScanState* node, MOTFdwStateSt* festate, TupleTableSlot* slot)
{
    MOT::RC rc = MOT::RC_OK;
    ForeignScan* fscan = (ForeignScan*)node->ss.ps.plan;
    festate->m_execExprs = (List*)ExecInitExpr((Expr*)fscan->fdw_exprs, (PlanState*)node);
    festate->m_econtext = node->ss.ps.ps_ExprContext;
    MOTAdaptor::CreateKeyBuffer(node->ss.ss_currentRelation, festate, 0);
    MOT::Sentinel* Sentinel =
        festate->m_bestIx->m_ix->IndexReadSentinel(&festate->m_stateKey[0], festate->m_currTxn->GetThdId());
    MOT::Row* currRow = festate->m_currTxn->RowLookup(festate->m_internalCmdOper, Sentinel, rc);

    if (currRow != NULL) {
        MOTAdaptor::UnpackRow(slot, festate->m_table, festate->m_attrsUsed, const_cast<uint8_t*>(currRow->GetData()));
        node->ss.is_scan_end = true;
        fscan->scan.scan_qual_optimized = true;
        ExecStoreVirtualTuple(slot);
        if (festate->m_ctidNum > 0) {
            HeapTuple resultTup = ExecFetchSlotTuple(slot);
            MOTRecConvertSt cv;
            cv.m_u.m_ptr = (uint64_t)currRow->GetPrimarySentinel();
            resultTup->t_self = cv.m_u.m_self;
            HeapTupleSetXmin(resultTup, InvalidTransactionId);
            HeapTupleSetXmax(resultTup, InvalidTransactionId);
            HeapTupleHeaderSetCmin(resultTup->t_data, InvalidTransactionId);
        }
        festate->m_rowsFound++;
        return slot;
    }
    if (rc != MOT::RC_OK) {
        if (MOT_IS_SEVERE()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "MOTIterateForeignScan", "Failed to lookup row");
            MOT_LOG_ERROR_STACK("Failed to lookup row");
        }
        CleanQueryStatesOnError(festate->m_currTxn);
        report_pg_error(rc,
            (void*)(festate->m_currTxn->m_errIx != nullptr ? festate->m_currTxn->m_errIx->GetName().c_str()
                                                           : "unknown"),
            (void*)festate->m_currTxn->m_errMsgBuf);
        return nullptr;
    }
    return nullptr;
}

/*
 *
 */
static TupleTableSlot* MOTIterateForeignScan(ForeignScanState* node)
{
    MOT::RC rc = MOT::RC_OK;
    if (node->ss.is_scan_end) {
        return nullptr;
    }

    MOT::Row* currRow = nullptr;
    MOTFdwStateSt* festate = (MOTFdwStateSt*)node->fdw_state;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    bool found = false;
    bool stopAtFirst = (festate->m_bestIx && festate->m_bestIx->m_ixOpers[0] == KEY_OPER::READ_KEY_EXACT &&
                        festate->m_bestIx->m_ix->GetUnique() == true);

    (void)ExecClearTuple(slot);

    if (stopAtFirst) {
        return IterateForeignScanStopAtFirst(node, festate, slot);
    }

    if (!festate->m_cursorOpened) {
        ForeignScan* fscan = (ForeignScan*)node->ss.ps.plan;
        festate->m_execExprs = (List*)ExecInitExpr((Expr*)fscan->fdw_exprs, (PlanState*)node);
        festate->m_econtext = node->ss.ps.ps_ExprContext;
        CleanCursors(festate);
        MOTAdaptor::OpenCursor(node->ss.ss_currentRelation, festate);

        festate->m_cursorOpened = true;
    }
    /*
     * The protocol for loading a virtual tuple into a slot is first
     * ExecClearTuple, then fill the values/isnull arrays, then
     * ExecStoreVirtualTuple.  If we don't find another row in the file, we
     * just skip the last step, leaving the slot empty as required.
     *
     * We can pass ExprContext = NULL because we read all columns from the
     * file, so no need to evaluate default expressions.
     *
     * We can also pass tupleOid = NULL because we don't allow oids for
     * foreign tables.
     */
    // festate->cursor[1] might be NULL (in case it is not in use)
    if (festate->m_cursor[0] == nullptr || !festate->m_cursor[0]->IsValid() ||
        (festate->m_cursor[1] != nullptr && !festate->m_cursor[1]->IsValid())) {
        return nullptr;
    }

    do {
        MOT::Sentinel* Sentinel = festate->m_cursor[0]->GetPrimarySentinel();
        currRow = festate->m_currTxn->RowLookup(festate->m_internalCmdOper, Sentinel, rc);
        if (currRow == NULL) {
            if (rc != MOT::RC_OK) {
                if (MOT_IS_SEVERE()) {
                    MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "MOTIterateForeignScan", "Failed to lookup row");
                    MOT_LOG_ERROR_STACK("Failed to lookup row");
                }

                CleanQueryStatesOnError(festate->m_currTxn);
                report_pg_error(rc,
                    (void*)(festate->m_currTxn->m_errIx != NULL ? festate->m_currTxn->m_errIx->GetName().c_str()
                                                                : "unknown"),
                    (void*)festate->m_currTxn->m_errMsgBuf);
                return NULL;
            }
            festate->m_cursor[0]->Next();
            continue;
        }

        // check end condition for range search
        if (MOTAdaptor::IsScanEnd(festate)) {
            festate->m_cursor[0]->Invalidate();
            node->ss.is_scan_end = true;
            break;
        }

        MOTAdaptor::UnpackRow(slot, festate->m_table, festate->m_attrsUsed, const_cast<uint8_t*>(currRow->GetData()));
        found = true;

        festate->m_cursor[0]->Next();
        break;
    } while (festate->m_cursor[0]->IsValid());

    if (found) {
        ExecStoreVirtualTuple(slot);

        if (festate->m_ctidNum > 0) {
            HeapTuple resultTup = ExecFetchSlotTuple(slot);
            MOTRecConvertSt cv;
            cv.m_u.m_ptr = (uint64_t)currRow->GetPrimarySentinel();
            resultTup->t_self = cv.m_u.m_self;
            HeapTupleSetXmin(resultTup, InvalidTransactionId);
            HeapTupleSetXmax(resultTup, InvalidTransactionId);
            HeapTupleHeaderSetCmin(resultTup->t_data, InvalidTransactionId);
        }
        festate->m_rowsFound++;
        return slot;
    } else {
        return nullptr;
    }
}

/*
 *
 */
static void MOTReScanForeignScan(ForeignScanState* node)
{
    MOTFdwStateSt* festate = (MOTFdwStateSt*)node->fdw_state;

    bool stopAtFirst = (festate->m_bestIx && festate->m_bestIx->m_ixOpers[0] == KEY_OPER::READ_KEY_EXACT &&
                        festate->m_bestIx->m_ix->GetUnique() == true);

    node->ss.is_scan_end = false;

    CleanCursors(festate);
    if (!stopAtFirst) {
        if (festate->m_execExprs == NULL) {
            ForeignScan* fscan = (ForeignScan*)node->ss.ps.plan;
            festate->m_execExprs = (List*)ExecInitExpr((Expr*)fscan->fdw_exprs, (PlanState*)node);
            festate->m_econtext = node->ss.ps.ps_ExprContext;
        }
        MOTAdaptor::OpenCursor(node->ss.ss_currentRelation, festate);
        festate->m_cursorOpened = true;
    }
}

/*
 *
 */
static void MOTEndForeignScan(ForeignScanState* node)
{
    MOTFdwStateSt* festate = (MOTFdwStateSt*)node->fdw_state;
    if (festate->m_allocInScan) {
        ReleaseFdwState(festate);
        node->fdw_state = NULL;
    }
}

/*
 * MOTAddForeignUpdateTargets
 * Add resjunk column(s) needed for update/delete on a foreign table
 */
static void MOTAddForeignUpdateTargets(Query* parsetree, RangeTblEntry* targetRte, Relation targetRelation)
{
    Var* var;
    const char* attrname;
    TargetEntry* tle;

    /* Make a Var representing the desired value */
    var = makeVar(parsetree->resultRelation, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);

    /* Wrap it in a resjunk TLE with the right name ... */
    attrname = MOT_REC_TID_NAME;

    tle = makeTargetEntry((Expr*)var, list_length(parsetree->targetList) + 1, pstrdup(attrname), true);

    /* ... and add it to the query's targetlist */
    parsetree->targetList = lappend(parsetree->targetList, tle);
}

static int MOTAcquireSampleRowsFunc(Relation relation, int elevel, HeapTuple* rows, int targrows, double* totalrows,
    double* totaldeadrows, void* additionalData, bool estimateTableRowNum)
{
    MOT::RC rc = MOT::RC_OK;
    int numrows = 0; /* # of sample rows collected */
    /* for random sampling */
    double samplerows = 0;                              /* # of rows fetched */
    double rowstoskip = -1;                             /* # of rows to skip before next sample */
    double rstate = anl_init_selection_state(targrows); /* random state */
    MOT::TxnManager* currTxn = GetSafeTxn(__FUNCTION__);
    MOT::Table* table = currTxn->GetTableByExternalId(RelationGetRelid(relation));
    if (table == nullptr) {
        abortParentTransactionParamsNoDetail(
            ERRCODE_UNDEFINED_TABLE, MOT_TABLE_NOTFOUND, (char*)RelationGetRelationName(relation));
        return 0;
    }
    MOT::IndexIterator* cursor = table->Begin(currTxn->GetThdId());
    TupleDesc desc = RelationGetDescr(relation);
    TupleTableSlot* slot = MakeSingleTupleTableSlot(desc);
    uint8_t attrsUsed[8];
    int pos = 0;
    MOT::Row* row = nullptr;

    for (int i = 0; i < desc->natts; i++) {
        if (!desc->attrs[i]->attisdropped) {
            BITMAP_SET(attrsUsed, (desc->attrs[i]->attnum - 1));
        }
    }

    while (cursor->IsValid()) {
        row = NULL;

        MOT::Sentinel* Sentinel = cursor->GetPrimarySentinel();
        row = currTxn->RowLookup(MOT::AccessType::RD, Sentinel, rc);
        cursor->Next();

        if (row == NULL) {
            continue;
        }

        /* Always increment sample row counter. */
        samplerows += 1;

        /*
         * Determine the slot where this sample row should be stored.  Set pos to
         * negative value to indicate the row should be skipped.
         */
        if (numrows < targrows) {
            /* First targrows rows are always included into the sample */
            pos = numrows++;
        } else {
            /*
             * Now we start replacing tuples in the sample until we reach the end
             * of the relation.  Same algorithm as in acquire_sample_rows in
             * analyze.c; see Jeff Vitter's paper.
             */
            if (rowstoskip < 0)
                rowstoskip = anl_get_next_S(samplerows, targrows, &rstate);

            if (rowstoskip <= 0) {
                /* Choose a random reservoir element to replace. */
                pos = (int)(targrows * anl_random_fract());
                Assert(pos >= 0 && pos < targrows);
                heap_freetuple(rows[pos]);
            } else {
                /* Skip this tuple. */
                pos = -1;
            }

            rowstoskip -= 1;
        }

        if (pos >= 0) {
            /*
             * Create sample tuple from current result row, and store it in the
             * position determined above.  The tuple has to be created in anl_cxt.
             */
            (void)ExecClearTuple(slot);
            MOTAdaptor::UnpackRow(slot, table, attrsUsed, const_cast<uint8_t*>(row->GetData()));
            ExecStoreVirtualTuple(slot);
            rows[pos] = ExecCopySlotTuple(slot);
        }
    }

    /* clean up */
    ExecDropSingleTupleTableSlot(slot);

    if (cursor != NULL) {
        cursor->Invalidate();
        cursor->Destroy();
        delete cursor;
    }

    /* We assume that we have no dead tuple. */
    *totaldeadrows = 0.0;

    /* We've retrieved all living tuples from foreign server. */
    *totalrows = samplerows;

    /*
     * Emit some interesting relation info
     */
    ereport(elevel,
        (errmsg("\"%s\": table contains %.0f rows, %d rows in sample",
            RelationGetRelationName(relation),
            samplerows,
            numrows)));

    return numrows;
}

/*
 *
 */
static bool MOTAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages,
    void* additionalData, bool estimateTableRowNum)
{
    /* Return the row-analysis function pointer */
    *func = MOTAcquireSampleRowsFunc;
    *totalpages = 1;

    return true;
}

List* MOTPlanForeignModify(PlannerInfo* root, ModifyTable* plan, ::Index resultRelation, int subplanIndex)
{
    switch (plan->operation) {
        case CMD_INSERT:
        case CMD_UPDATE:
            isMemoryLimitReached();
            break;
        default:
            break;
    }

    MOTFdwStateSt* fdwState = nullptr;
    RangeTblEntry* rte = planner_rt_fetch(resultRelation, root);
    Relation rel = heap_open(rte->relid, NoLock);
    TupleDesc desc = RelationGetDescr(rel);
    uint8_t attrsModify[BITMAP_GETLEN(desc->natts)];
    uint8_t* ptrAttrsModify = attrsModify;
    MOT::TxnManager* currTxn = GetSafeTxn(__FUNCTION__);
    MOT::Table* table = currTxn->GetTableByExternalId(RelationGetRelid(rel));

    if ((int)resultRelation < root->simple_rel_array_size && root->simple_rel_array[resultRelation] != nullptr) {
        if (root->simple_rel_array[resultRelation]->fdw_private != nullptr) {
            fdwState = (MOTFdwStateSt*)root->simple_rel_array[resultRelation]->fdw_private;
            ptrAttrsModify = fdwState->m_attrsUsed;
        }
    } else {
        fdwState = (MOTFdwStateSt*)palloc0(sizeof(MOTFdwStateSt));
        fdwState->m_cmdOper = plan->operation;
        fdwState->m_foreignTableId = rte->relid;
        fdwState->m_numAttrs = RelationGetNumberOfAttributes(rel);

        fdwState->m_table = table;
        if (fdwState->m_table == nullptr) {
            abortParentTransactionParamsNoDetail(
                ERRCODE_UNDEFINED_TABLE, MOT_TABLE_NOTFOUND, (char*)RelationGetRelationName(rel));
        }

        int len = BITMAP_GETLEN(fdwState->m_numAttrs);
        fdwState->m_attrsUsed = (uint8_t*)palloc0(len);
        fdwState->m_attrsModified = (uint8_t*)palloc0(len);
    }

    switch (plan->operation) {
        case CMD_INSERT: {
            for (int i = 0; i < desc->natts; i++) {
                if (!desc->attrs[i]->attisdropped) {
                    BITMAP_SET(fdwState->m_attrsUsed, (desc->attrs[i]->attnum - 1));
                }
            }
            break;
        }
        case CMD_UPDATE: {
            errno_t erc = memset_s(attrsModify, BITMAP_GETLEN(desc->natts), 0, BITMAP_GETLEN(desc->natts));
            securec_check(erc, "\0", "\0");
            for (int i = 0; i < desc->natts; i++) {
                if (bms_is_member(desc->attrs[i]->attnum - FirstLowInvalidHeapAttributeNumber, rte->updatedCols)) {
                    BITMAP_SET(ptrAttrsModify, (desc->attrs[i]->attnum - 1));
                }
            }
            break;
        }
        case CMD_DELETE: {
            if (list_length(plan->returningLists) > 0) {
                errno_t erc = memset_s(attrsModify, BITMAP_GETLEN(desc->natts), 0, BITMAP_GETLEN(desc->natts));
                securec_check(erc, "\0", "\0");
                for (int i = 0; i < desc->natts; i++) {
                    if (!desc->attrs[i]->attisdropped) {
                        BITMAP_SET(ptrAttrsModify, (desc->attrs[i]->attnum - 1));
                    }
                }
            }
            break;
        }
        default:
            break;
    }

    heap_close(rel, NoLock);

    return ((fdwState == nullptr) ? (List*)BitmapSerialize(nullptr, attrsModify, BITMAP_GETLEN(desc->natts))
                                  : (List*)SerializeFdwState(fdwState));
}

static TupleTableSlot* MOTExecForeignInsert(
    EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot)
{
    MOTFdwStateSt* fdwState = (MOTFdwStateSt*)resultRelInfo->ri_FdwState;
    MOT::RC rc = MOT::RC_OK;

    if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached() && fdwState != nullptr) {
        CleanQueryStatesOnError(fdwState->m_currTxn);
    }

    isMemoryLimitReached();

    if (fdwState == nullptr) {
        fdwState = (MOTFdwStateSt*)palloc0(sizeof(MOTFdwStateSt));
        fdwState->m_txnId = GetCurrentTransactionIdIfAny();
        fdwState->m_currTxn = GetSafeTxn(__FUNCTION__);
        fdwState->m_table = fdwState->m_currTxn->GetTableByExternalId(RelationGetRelid(resultRelInfo->ri_RelationDesc));
        if (fdwState->m_table == nullptr) {
            pfree(fdwState);
            report_pg_error(MOT::RC_TABLE_NOT_FOUND);
            return nullptr;
        }
        fdwState->m_numAttrs = RelationGetNumberOfAttributes(resultRelInfo->ri_RelationDesc);

        int len = BITMAP_GETLEN(fdwState->m_numAttrs);
        fdwState->m_attrsUsed = (uint8_t*)palloc0(len);
        fdwState->m_attrsModified = (uint8_t*)palloc0(len);
        errno_t erc = memset_s(fdwState->m_attrsUsed, len, 0xff, len);
        securec_check(erc, "\0", "\0");
        resultRelInfo->ri_FdwState = fdwState;
    }

    if ((rc = MOTAdaptor::InsertRow(fdwState, slot)) == MOT::RC_OK) {
        estate->es_processed++;
        if (resultRelInfo->ri_projectReturning)
            return slot;
        else
            return nullptr;
    } else {
        if (MOT_IS_SEVERE()) {
            MOT_REPORT_ERROR(MOT_ERROR_INTERNAL, "MOTExecForeignInsert", "Failed to insert row");
            MOT_LOG_ERROR_STACK("Failed to insert row");
        }
        elog(DEBUG2, "Abort parent transaction from MOT insert, id %lu", fdwState->m_txnId);
        CleanQueryStatesOnError(fdwState->m_currTxn);
        report_pg_error(rc,
            (void*)(fdwState->m_currTxn->m_errIx != nullptr ? fdwState->m_currTxn->m_errIx->GetName().c_str()
                                                            : "unknown"),
            (void*)fdwState->m_currTxn->m_errMsgBuf);
        return nullptr;
    }
}

static TupleTableSlot* MOTExecForeignUpdate(
    EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot)
{
    MOTFdwStateSt* fdwState = (MOTFdwStateSt*)resultRelInfo->ri_FdwState;
    MOT::RC rc = MOT::RC_OK;
    MOT::Row* currRow = nullptr;
    AttrNumber num = fdwState->m_ctidNum - 1;
    MOTRecConvertSt cv;

    if (MOTAdaptor::m_engine->IsSoftMemoryLimitReached()) {
        CleanQueryStatesOnError(fdwState->m_currTxn);
    }
    isMemoryLimitReached();

    if (fdwState->m_ctidNum != 0 && planSlot->tts_nvalid >= fdwState->m_ctidNum && !planSlot->tts_isnull[num]) {
        cv.m_u.m_ptr = 0;
        cv.m_u.m_self = *(ItemPointerData*)planSlot->tts_values[num];
        currRow = fdwState->m_currTxn->RowLookup(fdwState->m_internalCmdOper, (MOT::Sentinel*)cv.m_u.m_ptr, rc);
    } else {
        elog(ERROR,
            "MOTExecForeignUpdate failed to fetch row for update ctid %d nvalid %d %s",
            num,
            planSlot->tts_nvalid,
            (planSlot->tts_isnull[num] ? "NULL" : "NOT NULL"));
        CleanQueryStatesOnError(fdwState->m_currTxn);
        report_pg_error(MOT::RC_ERROR);
        return nullptr;
    }

    if (currRow == nullptr) {
        elog(ERROR, "MOTExecForeignUpdate failed to fetch row");
        CleanQueryStatesOnError(fdwState->m_currTxn);
        report_pg_error(((rc == MOT::RC_OK) ? MOT::RC_ERROR : rc));
        return nullptr;
    }

    // This case handle multiple updates of the same row in one query
    if (fdwState->m_currTxn->IsUpdatedInCurrStmt()) {
        return nullptr;
    }
    if ((rc = MOTAdaptor::UpdateRow(fdwState, planSlot, currRow)) == MOT::RC_OK) {
        if (resultRelInfo->ri_projectReturning) {
            return planSlot;
        } else {
            estate->es_processed++;
            return nullptr;
        }
    } else {
        elog(DEBUG2, "Abort parent transaction from MOT update, id %lu", fdwState->m_txnId);
        CleanQueryStatesOnError(fdwState->m_currTxn);
        report_pg_error(rc,
            (void*)(fdwState->m_currTxn->m_errIx != nullptr ? fdwState->m_currTxn->m_errIx->GetName().c_str()
                                                            : "unknown"),
            (void*)fdwState->m_currTxn->m_errMsgBuf);
        return nullptr;
    }
}

static TupleTableSlot* MOTExecForeignDelete(
    EState* estate, ResultRelInfo* resultRelInfo, TupleTableSlot* slot, TupleTableSlot* planSlot)
{
    MOTFdwStateSt* fdwState = (MOTFdwStateSt*)resultRelInfo->ri_FdwState;
    MOT::RC rc = MOT::RC_OK;
    MOT::Row* currRow = nullptr;
    AttrNumber num = fdwState->m_ctidNum - 1;
    MOTRecConvertSt cv;

    if (fdwState->m_ctidNum != 0 && planSlot->tts_nvalid >= fdwState->m_ctidNum && !planSlot->tts_isnull[num]) {
        cv.m_u.m_ptr = 0;
        cv.m_u.m_self = *(ItemPointerData*)planSlot->tts_values[num];
        currRow = fdwState->m_currTxn->RowLookup(fdwState->m_internalCmdOper, (MOT::Sentinel*)cv.m_u.m_ptr, rc);
    } else {
        elog(ERROR,
            "MOTExecForeignDelete failed to fetch row for delete ctid %d nvalid %d %s",
            num,
            planSlot->tts_nvalid,
            (planSlot->tts_isnull[num] ? "NULL" : "NOT NULL"));
        CleanQueryStatesOnError(fdwState->m_currTxn);
        report_pg_error(MOT::RC_ERROR);
        return nullptr;
    }

    if (currRow == nullptr) {
        // This case handle multiple updates of the same row in one query
        if (fdwState->m_currTxn->IsUpdatedInCurrStmt()) {
            return nullptr;
        }
        elog(ERROR, "MOTExecForeignDelete failed to fetch row");
        CleanQueryStatesOnError(fdwState->m_currTxn);
        report_pg_error(((rc == MOT::RC_OK) ? MOT::RC_ERROR : rc));
        return nullptr;
    }

    if ((rc = MOTAdaptor::DeleteRow(fdwState, slot)) == MOT::RC_OK) {
        if (resultRelInfo->ri_projectReturning) {
            MOTAdaptor::UnpackRow(
                slot, fdwState->m_table, fdwState->m_attrsUsed, const_cast<uint8_t*>(currRow->GetData()));
            ExecStoreVirtualTuple(slot);
            return slot;
        } else {
            estate->es_processed++;
            return nullptr;
        }
    } else {
        elog(DEBUG2, "Abort parent transaction from MOT delete, id %lu", fdwState->m_txnId);
        CleanQueryStatesOnError(fdwState->m_currTxn);
        report_pg_error(rc,
            (void*)(fdwState->m_currTxn->m_errIx != nullptr ? fdwState->m_currTxn->m_errIx->GetName().c_str()
                                                            : "unknown"),
            (void*)fdwState->m_currTxn->m_errMsgBuf);
        return nullptr;
    }
}

static void MOTEndForeignModify(EState* estate, ResultRelInfo* resultRelInfo)
{
    MOTFdwStateSt* fdwState = (MOTFdwStateSt*)resultRelInfo->ri_FdwState;

    if (fdwState->m_allocInScan == false) {
        ReleaseFdwState(fdwState);
        resultRelInfo->ri_FdwState = NULL;
    }
}

static void MOTXactCallback(XactEvent event, void* arg)
{
    int rc = MOT::RC_OK;
    MOT::TxnManager* txn = nullptr;

    PG_TRY();
    {
        txn = GetSafeTxn(__FUNCTION__);
    }
    PG_CATCH();
    {
        switch (event) {
            case XACT_EVENT_ABORT:
            case XACT_EVENT_ROLLBACK_PREPARED:
            case XACT_EVENT_PREROLLBACK_CLEANUP:
                elog(LOG, "Failed to get MOT transaction manager during abort.");
                return;
            default:
                PG_RE_THROW();
                return;
        }
    }
    PG_END_TRY();

    ::TransactionId tid = GetCurrentTransactionIdIfAny();
    if (TransactionIdIsValid(tid)) {
        txn->SetTransactionId(tid);
    }

    MOT::TxnState txnState = txn->GetTxnState();

    elog(DEBUG2, "xact_callback event %u, transaction state %u, tid %lu", event, txnState, tid);

    if (event == XACT_EVENT_START) {
        elog(DEBUG2, "XACT_EVENT_START, tid %lu", tid);
        if (txnState == MOT::TxnState::TXN_START) {
            // Double start!!!
            MOTAdaptor::Rollback();
        }
        if (txnState != MOT::TxnState::TXN_PREPARE) {
            txn->StartTransaction(tid, u_sess->utils_cxt.XactIsoLevel);
        }
    } else if (event == XACT_EVENT_COMMIT) {
        if (txnState == MOT::TxnState::TXN_END_TRANSACTION) {
            elog(DEBUG2, "XACT_EVENT_COMMIT, transaction already in end state, skipping, tid %lu", tid);
            return;
        }

        if (txnState == MOT::TxnState::TXN_COMMIT) {
            elog(DEBUG2, "XACT_EVENT_COMMIT, transaction already in commit state, skipping, tid %lu", tid);
            return;
        }

        if (txnState == MOT::TxnState::TXN_PREPARE) {
            // Transaction is in prepare state, it's 2pc transaction. Nothing to do in XACT_EVENT_COMMIT.
            // Actual CommitPrepared will be done when XACT_EVENT_RECORD_COMMIT is called by the envelope.
            elog(DEBUG2,
                "XACT_EVENT_COMMIT, transaction is in prepare state, awaiting XACT_EVENT_RECORD_COMMIT, tid %lu",
                tid);
            return;
        }

        elog(DEBUG2, "XACT_EVENT_COMMIT, tid %lu", tid);

        rc = MOTAdaptor::ValidateCommit();
        if (rc != MOT::RC_OK) {
            elog(DEBUG2, "commit failed");
            elog(DEBUG2, "Abort parent transaction from MOT commit, tid %lu", tid);
            MemoryEreportError();
            abortParentTransactionParamsNoDetail(ERRCODE_T_R_SERIALIZATION_FAILURE,
                "Commit: could not serialize access due to concurrent update(%d)",
                txnState);
        }
        txn->SetTxnState(MOT::TxnState::TXN_COMMIT);
    } else if (event == XACT_EVENT_RECORD_COMMIT) {
        if (txnState == MOT::TxnState::TXN_END_TRANSACTION) {
            elog(DEBUG2, "XACT_EVENT_COMMIT, transaction already in end state, skipping, tid %lu", tid);
            return;
        }

        MOT_ASSERT(txnState == MOT::TxnState::TXN_COMMIT || txnState == MOT::TxnState::TXN_PREPARE);
        elog(DEBUG2, "XACT_EVENT_RECORD_COMMIT, tid %lu", tid);

        // Need to get the envelope CSN for cross transaction support.
        uint64_t csn = MOT::GetCSNManager().GetNextCSN();
        if (txnState == MOT::TxnState::TXN_PREPARE) {
            MOTAdaptor::CommitPrepared(csn);
        } else {
            MOTAdaptor::RecordCommit(csn);
        }
    } else if (event == XACT_EVENT_END_TRANSACTION) {
        if (txnState == MOT::TxnState::TXN_END_TRANSACTION) {
            elog(DEBUG2, "XACT_EVENT_END_TRANSACTION, transaction already in end state, skipping, tid %lu", tid);
            return;
        }
        elog(DEBUG2, "XACT_EVENT_END_TRANSACTION, tid %lu", tid);
        MOTAdaptor::EndTransaction();
        txn->SetTxnState(MOT::TxnState::TXN_END_TRANSACTION);
    } else if (event == XACT_EVENT_PREPARE) {
        elog(DEBUG2, "XACT_EVENT_PREPARE, tid %lu", tid);
        rc = MOTAdaptor::Prepare();
        if (rc != MOT::RC_OK) {
            elog(DEBUG2, "prepare failed");
            elog(DEBUG2, "Abort parent transaction from MOT prepare, tid %lu", tid);
            MemoryEreportError();
            abortParentTransactionParamsNoDetail(ERRCODE_T_R_SERIALIZATION_FAILURE,
                "Prepare: could not serialize access due to concurrent update(%u)",
                txnState);
        }
        txn->SetTxnState(MOT::TxnState::TXN_PREPARE);
    } else if (event == XACT_EVENT_ABORT) {
        elog(DEBUG2, "XACT_EVENT_ABORT, tid %lu", tid);
        MOTAdaptor::Rollback();
        txn->SetTxnState(MOT::TxnState::TXN_ROLLBACK);
    } else if (event == XACT_EVENT_COMMIT_PREPARED) {
        if (txnState == MOT::TxnState::TXN_PREPARE) {
            elog(DEBUG2, "XACT_EVENT_COMMIT_PREPARED, tid %lu", tid);
            // Need to get the envelope CSN for cross transaction support.
            uint64_t csn = MOT::GetCSNManager().GetNextCSN();
            MOTAdaptor::CommitPrepared(csn);
        } else if (txnState == MOT::TxnState::TXN_START) {
            elog(DEBUG2, "XACT_EVENT_COMMIT_PREPARED, tid %lu", tid);
            // Need to get the envelope CSN for cross transaction support.
            uint64_t csn = MOT::GetCSNManager().GetNextCSN();
            rc = MOTAdaptor::Commit(csn);
        } else if (txnState != MOT::TxnState::TXN_ROLLBACK && txnState != MOT::TxnState::TXN_COMMIT) {
            elog(DEBUG2, "XACT_EVENT_COMMIT_PREPARED, tid %lu", tid);
            abortParentTransactionParamsNoDetail(
                ERRCODE_T_R_SERIALIZATION_FAILURE, "Commit Prepared: commit prepared without prepare (%u)", txnState);
        } else {
            rc = MOT::RC_OK;
        }
        if (rc != MOT::RC_OK) {
            elog(DEBUG2, "commit prepared failed");
            elog(DEBUG2, "Abort parent transaction from MOT commit prepared, tid %lu", tid);
            MemoryEreportError();
            abortParentTransactionParamsNoDetail(ERRCODE_T_R_SERIALIZATION_FAILURE,
                "Commit: could not serialize access due to concurrent update(%u)",
                txnState);
        }
        txn->SetTxnState(MOT::TxnState::TXN_COMMIT);
    } else if (event == XACT_EVENT_ROLLBACK_PREPARED) {
        elog(DEBUG2, "XACT_EVENT_ROLLBACK_PREPARED, tid %lu", tid);
        if (txnState != MOT::TxnState::TXN_PREPARE) {
            elog(DEBUG2, "Rollback prepared and txn is not in prepare state, tid %lu", tid);
        }
        MOTAdaptor::RollbackPrepared();
        txn->SetTxnState(MOT::TxnState::TXN_ROLLBACK);
    } else if (event == XACT_EVENT_PREROLLBACK_CLEANUP) {
        CleanQueryStatesOnError(txn);
    }
}

static void MOTSubxactCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void* arg)
{
    return;
}

static int MOTXlateCheckpointErr(int err)
{
    int code = 0;
    switch (err) {
        case MOT::CheckpointWorkerPool::ErrCodes::SUCCESS:
            code = ERRCODE_SUCCESSFUL_COMPLETION;
            break;
        case MOT::CheckpointWorkerPool::ErrCodes::FILE_IO:
            code = ERRCODE_IO_ERROR;
            break;
        case MOT::CheckpointWorkerPool::ErrCodes::MEMORY:
            code = ERRCODE_INSUFFICIENT_RESOURCES;
            break;
        case MOT::CheckpointWorkerPool::ErrCodes::TABLE:
        case MOT::CheckpointWorkerPool::ErrCodes::INDEX:
        case MOT::CheckpointWorkerPool::ErrCodes::CALC:
            code = ERRCODE_INTERNAL_ERROR;
            break;
        default:
            break;
    }
    return code;
}

static void MOTCheckpointCallback(CheckpointEvent checkpointEvent, uint64_t lsn, void* arg)
{
    bool status = true;
    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    if (engine == nullptr) {
        elog(INFO, "Failed to create MOT engine");
        return;
    }

    switch (checkpointEvent) {
        case EVENT_CHECKPOINT_CREATE_SNAPSHOT:
            status = engine->CreateSnapshot();
            break;
        case EVENT_CHECKPOINT_SNAPSHOT_READY:
            status = engine->SnapshotReady(lsn);
            break;
        case EVENT_CHECKPOINT_BEGIN_CHECKPOINT:
            status = engine->BeginCheckpoint();
            break;
        case EVENT_CHECKPOINT_ABORT:
            status = engine->AbortCheckpoint();
            break;
        default:
            // unknown event
            status = false;
            break;
    }

    if (!status) {
        // we treat errors fatally.
        ereport(PANIC,
            (MOTXlateCheckpointErr(engine->GetCheckpointErrCode()), errmsg("%s", engine->GetCheckpointErrStr())));
    }
}

/* @MOT
 * brief: Validate table definition
 * input param @obj: A Obj including infomation to validate when alter tabel and create table.
 */
static void MOTValidateTableDef(Node* obj)
{
    ::TransactionId tid = GetCurrentTransactionId();
    if (obj == nullptr) {
        return;
    }

    switch (nodeTag(obj)) {
        case T_AlterTableStmt: {
            AlterTableStmt* ats = (AlterTableStmt*)obj;
            AlterTableCmd* cmd = NULL;
            bool allow = false;
            if (list_length(ats->cmds) == 1) {
                ListCell* cell = list_head(ats->cmds);
                cmd = (AlterTableCmd*)lfirst(cell);
                if (cmd->subtype == AT_ChangeOwner) {
                    allow = true;
                } else if (cmd->subtype == AT_AddIndex) {
                    allow = true;
                }
            }
            if (allow == false) {
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                        errmodule(MOD_MOT),
                        errmsg("Alter table operation is not supported for memory table.")));
            }
            break;
        }
        case T_CreateForeignTableStmt: {
            isMemoryLimitReached();
            if (g_instance.attr.attr_storage.enableIncrementalCheckpoint == true) {
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                        errmodule(MOD_MOT),
                        errmsg("Cannot create MOT tables while incremental checkpoint is enabled.")));
            }

            MOTAdaptor::CreateTable((CreateForeignTableStmt*)obj, tid);
            break;
        }
        case T_IndexStmt: {
            isMemoryLimitReached();
            MOTAdaptor::CreateIndex((IndexStmt*)obj, tid);
            break;
        }
        case T_ReindexStmt: {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                    errmodule(MOD_MOT),
                    errmsg("Reindex is not supported for memory table.")));
            break;
        }
        case T_DropForeignStmt: {
            DropForeignStmt* stmt = (DropForeignStmt*)obj;
            switch (stmt->relkind) {
                case RELKIND_INDEX:
                    MOTAdaptor::DropIndex(stmt, tid);
                    break;

                case RELKIND_RELATION:
                    MOTAdaptor::DropTable(stmt, tid);
                    break;
                default:
                    break;
            }
            break;
        }
        default:
            elog(ERROR, "unrecognized node type: %u", nodeTag(obj));
    }
}

static void MOTTruncateForeignTable(TruncateStmt* stmt, Relation rel)
{
    ::TransactionId tid = GetCurrentTransactionId();
    MOT::RC rc = MOTAdaptor::TruncateTable(rel, tid);
    if (rc != MOT::RC_OK) {
        MOT_LOG_ERROR_STACK("Failed to truncate table");
    }
    if (rc == MOT::RC::RC_NA) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_OPERATION_NOT_SUPPORTED),
                errmodule(MOD_MOT),
                errmsg("A checkpoint is in progress - cannot truncate table.")));
    } else {
        report_pg_error(rc);
    }
}

static void MOTVacuumForeignTable(VacuumStmt* stmt, Relation rel)
{
    if (stmt->options & VACOPT_AUTOVAC) {
        elog(LOG,
            "skipping vacuum table %s, oid: %u, vacuum initiated by autovacuum",
            NameStr(rel->rd_rel->relname),
            rel->rd_id);
        return;
    }
    ::TransactionId tid = GetCurrentTransactionId();

    PG_TRY();
    {
        MOTAdaptor::VacuumTable(rel, tid);
    }
    PG_CATCH();
    {
        elog(LOG, "Vacuum of table %s failed", NameStr(rel->rd_rel->relname));
        return;
    }
    PG_END_TRY();
}

static uint64_t MOTGetForeignRelationMemSize(Oid reloid, Oid ixoid)
{
    return MOTAdaptor::GetTableIndexSize(reloid, ixoid);
}

static MotMemoryDetail* MOTGetForeignMemSize(uint32_t* nodeCount, bool isGlobal)
{
    return MOTAdaptor::GetMemSize(nodeCount, isGlobal);
}

static MotSessionMemoryDetail* MOTGetForeignSessionMemSize(uint32_t* sessionCount)
{
    return MOTAdaptor::GetSessionMemSize(sessionCount);
}

static void MOTNotifyForeignConfigChange()
{
    MOTAdaptor::NotifyConfigChange();
}

static int MOTIsForeignRelationUpdatable(Relation rel)
{
    return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

static void InitMOTHandler()
{
    MOTAdaptor::Init();
    MOT::GetGlobalConfiguration().m_enableIncrementalCheckpoint =
        g_instance.attr.attr_storage.enableIncrementalCheckpoint;

    // if incremental checkpoint is enabled, do not register our callbacks
    if (MOT::GetGlobalConfiguration().m_enableIncrementalCheckpoint == false) {
        // Register our checkpoint and redo callbacks to the envelope.
        if (!MOTAdaptor::m_callbacks_initialized) {
            if (MOT::GetGlobalConfiguration().m_enableCheckpoint) {
                RegisterCheckpointCallback(MOTCheckpointCallback, NULL);
            } else {
                elog(WARNING, "MOT Checkpoint is disabled");
            }

            RegisterRedoCommitCallback(RedoTransactionCommit, NULL);
        }

        // Register CLOG callback to our recovery manager.
        MOT::GetRecoveryManager()->SetCommitLogCallback(&GetTransactionStateCallback);
        MOTAdaptor::m_callbacks_initialized = true;
    }
}

void MOTCheckpointFetchLock()
{
    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    if (engine != nullptr) {
        elog(LOG, "MOT Checkpoint FetchRdLock");
        engine->GetCheckpointManager()->FetchRdLock();
    }
}

void MOTCheckpointFetchUnlock()
{
    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    if (engine != nullptr) {
        engine->GetCheckpointManager()->FetchRdUnlock();
        elog(LOG, "MOT Checkpoint FetchRdUnlock");
    }
}

bool MOTCheckpointExists(
    char* ctrlFilePath, size_t ctrlLen, char* checkpointDir, size_t checkpointLen, size_t& basePathLen)
{
    MOT::MOTEngine* engine = MOT::MOTEngine::GetInstance();
    if (engine == nullptr) {
        return false;
    }

    MOT::CheckpointManager* checkpointManager = engine->GetCheckpointManager();
    if (checkpointManager == nullptr) {
        return false;
    }

    if (checkpointManager->GetId() == MOT::CheckpointControlFile::invalidId) {
        return false;
    }

    if (MOT::GetGlobalConfiguration().m_enableIncrementalCheckpoint == true) {
        return false;
    }

    std::string workingDir;
    if (checkpointManager->GetCheckpointWorkingDir(workingDir) == false) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmodule(MOD_MOT), errmsg("Failed to obtain working dir")));
        return false;
    }

    std::string dirName;
    if (checkpointManager->GetCheckpointDirName(dirName) == false) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmodule(MOD_MOT), errmsg("Failed to obtain dir name")));
        return false;
    }

    errno_t rc =
        snprintf_s(checkpointDir, checkpointLen, checkpointLen - 1, "%s%s", workingDir.c_str(), dirName.c_str());
    securec_check_ss(rc, "", "");
    rc = snprintf_s(
        ctrlFilePath, ctrlLen, ctrlLen - 1, "%s%s", workingDir.c_str(), MOT::CheckpointControlFile::CTRL_FILE_NAME);
    securec_check_ss(rc, "", "");

    basePathLen = workingDir.length() - 1;
    return true;
}

inline bool IsNotEqualOper(OpExpr* op)
{
    switch (op->opno) {
        case INT48NEOID:
        case BooleanNotEqualOperator:
        case 402:
        case INT8NEOID:
        case INT84NEOID:
        case INT4NEOID:
        case INT2NEOID:
        case 531:
        case INT24NEOID:
        case INT42NEOID:
        case 561:
        case 567:
        case 576:
        case 608:
        case 644:
        case FLOAT4NEOID:
        case 630:
        case 5514:
        case 643:
        case FLOAT8NEOID:
        case 713:
        case 812:
        case 901:
        case BPCHARNEOID:
        case 1071:
        case DATENEOID:
        case 1109:
        case 1551:
        case FLOAT48NEOID:
        case FLOAT84NEOID:
        case 1321:
        case 1331:
        case 1501:
        case 1586:
        case 1221:
        case 1202:
        case NUMERICNEOID:
        case 1785:
        case 1805:
        case INT28NEOID:
        case INT82NEOID:
        case 1956:
        case 3799:
        case TIMESTAMPNEOID:
        case 2350:
        case 2363:
        case 2376:
        case 2389:
        case 2539:
        case 2545:
        case 2973:
        case 3517:
        case 3630:
        case 3677:
        case 2989:
        case 3883:
        case 5551:
            return true;

        default:
            return false;
    }
}

inline void RevertKeyOperation(KEY_OPER& oper)
{
    if (oper == KEY_OPER::READ_KEY_BEFORE) {
        oper = KEY_OPER::READ_KEY_AFTER;
    } else if (oper == KEY_OPER::READ_KEY_OR_PREV) {
        oper = KEY_OPER::READ_KEY_OR_NEXT;
    } else if (oper == KEY_OPER::READ_KEY_AFTER) {
        oper = KEY_OPER::READ_KEY_BEFORE;
    } else if (oper == KEY_OPER::READ_KEY_OR_NEXT) {
        oper = KEY_OPER::READ_KEY_OR_PREV;
    }
    return;
}

inline bool GetKeyOperation(OpExpr* op, KEY_OPER& oper)
{
    switch (op->opno) {
        case FLOAT8EQOID:
        case FLOAT4EQOID:
        case INT2EQOID:
        case INT4EQOID:
        case INT8EQOID:
        case INT24EQOID:
        case INT42EQOID:
        case INT84EQOID:
        case INT48EQOID:
        case INT28EQOID:
        case INT82EQOID:
        case FLOAT48EQOID:
        case FLOAT84EQOID:
        case 5513:  // INT1EQ
        case BPCHAREQOID:
        case TEXTEQOID:
        case 92:    // CHAREQ
        case 2536:  // timestampVStimestamptz
        case 2542:  // timestamptzVStimestamp
        case 2347:  // dateVStimestamp
        case 2360:  // dateVStimestamptz
        case 2373:  // timestampVSdate
        case 2386:  // timestamptzVSdate
        case TIMESTAMPEQOID:
            oper = KEY_OPER::READ_KEY_EXACT;
            break;
        case FLOAT8LTOID:
        case FLOAT4LTOID:
        case INT2LTOID:
        case INT4LTOID:
        case INT8LTOID:
        case INT24LTOID:
        case INT42LTOID:
        case INT84LTOID:
        case INT48LTOID:
        case INT28LTOID:
        case INT82LTOID:
        case FLOAT48LTOID:
        case FLOAT84LTOID:
        case 5515:  // INT1LT
        case 1058:  // BPCHARLT
        case 631:   // CHARLT
        case TEXTLTOID:
        case 2534:  // timestampVStimestamptz
        case 2540:  // timestamptzVStimestamp
        case 2345:  // dateVStimestamp
        case 2358:  // dateVStimestamptz
        case 2371:  // timestampVSdate
        case 2384:  // timestamptzVSdate
        case TIMESTAMPLTOID:
            oper = KEY_OPER::READ_KEY_BEFORE;
            break;
        case FLOAT8LEOID:
        case FLOAT4LEOID:
        case INT2LEOID:
        case INT4LEOID:
        case INT8LEOID:
        case INT24LEOID:
        case INT42LEOID:
        case INT84LEOID:
        case INT48LEOID:
        case INT28LEOID:
        case INT82LEOID:
        case FLOAT48LEOID:
        case FLOAT84LEOID:
        case 5516:  // INT1LE
        case 1059:  // BPCHARLE
        case 632:   // CHARLE
        case 665:   // TEXTLE
        case 2535:  // timestampVStimestamptz
        case 2541:  // timestamptzVStimestamp
        case 2346:  // dateVStimestamp
        case 2359:  // dateVStimestamptz
        case 2372:  // timestampVSdate
        case 2385:  // timestamptzVSdate
        case TIMESTAMPLEOID:
            oper = KEY_OPER::READ_KEY_OR_PREV;
            break;
        case FLOAT8GTOID:
        case FLOAT4GTOID:
        case INT2GTOID:
        case INT4GTOID:
        case INT8GTOID:
        case INT24GTOID:
        case INT42GTOID:
        case INT84GTOID:
        case INT48GTOID:
        case INT28GTOID:
        case INT82GTOID:
        case FLOAT48GTOID:
        case FLOAT84GTOID:
        case 5517:       // INT1GT
        case 1060:       // BPCHARGT
        case 633:        // CHARGT
        case TEXTGTOID:  // TEXTGT
        case 2538:       // timestampVStimestamptz
        case 2544:       // timestamptzVStimestamp
        case 2349:       // dateVStimestamp
        case 2362:       // dateVStimestamptz
        case 2375:       // timestampVSdate
        case 2388:       // timestamptzVSdate
        case TIMESTAMPGTOID:
            oper = KEY_OPER::READ_KEY_AFTER;
            break;
        case FLOAT8GEOID:
        case FLOAT4GEOID:
        case INT2GEOID:
        case INT4GEOID:
        case INT8GEOID:
        case INT24GEOID:
        case INT42GEOID:
        case INT84GEOID:
        case INT48GEOID:
        case INT28GEOID:
        case INT82GEOID:
        case FLOAT48GEOID:
        case FLOAT84GEOID:
        case 5518:  // INT1GE
        case 1061:  // BPCHARGE
        case 634:   // CHARGE
        case 667:   // TEXTGE
        case 2537:  // timestampVStimestamptz
        case 2543:  // timestamptzVStimestamp
        case 2348:  // dateVStimestamp
        case 2361:  // dateVStimestamptz
        case 2374:  // timestampVSdate
        case 2387:  // timestamptzVSdate
        case TIMESTAMPGEOID:
            oper = KEY_OPER::READ_KEY_OR_NEXT;
            break;
        case OID_TEXT_LIKE_OP:
        case OID_BPCHAR_LIKE_OP:
            oper = KEY_OPER::READ_KEY_LIKE;
            break;
        default:
            oper = KEY_OPER::READ_INVALID;
            break;
    }

    return (oper != KEY_OPER::READ_INVALID);
}

bool IsSameRelation(Expr* expr, uint32_t id)
{
    switch (expr->type) {
        case T_Param:
        case T_Const: {
            return false;
        }
        case T_Var: {
            return (((Var*)expr)->varno == id);
        }
        case T_OpExpr: {
            OpExpr* op = (OpExpr*)expr;
            bool l = IsSameRelation((Expr*)linitial(op->args), id);
            bool r = IsSameRelation((Expr*)lsecond(op->args), id);
            return (l || r);
        }
        case T_FuncExpr: {
            FuncExpr* func = (FuncExpr*)expr;

            if (func->funcformat == COERCE_IMPLICIT_CAST || func->funcformat == COERCE_EXPLICIT_CAST) {
                return IsSameRelation((Expr*)linitial(func->args), id);
            } else if (list_length(func->args) == 0) {
                return false;
            } else {
                return true;
            }
        }
        case T_RelabelType: {
            return IsSameRelation(((RelabelType*)expr)->arg, id);
        }
        default:
            return true;
    }
}

bool IsMOTExpr(RelOptInfo* baserel, MOTFdwStateSt* state, MatchIndexArr* marr, Expr* expr, Expr** result, bool setLocal)
{
    /*
     * We only support the following operators and data types.
     */
    bool isOperatorMOTReady = false;

    switch (expr->type) {
        case T_Const: {
            if (result != nullptr)
                *result = expr;
            isOperatorMOTReady = true;
            break;
        }

        case T_Var: {
            if (result != nullptr)
                *result = expr;
            isOperatorMOTReady = true;
            break;
        }
        case T_Param: {
            if (result != nullptr)
                *result = expr;
            isOperatorMOTReady = true;
            break;
        }
        case T_OpExpr: {
            KEY_OPER oper;
            OpExpr* op = (OpExpr*)expr;
            Expr* l = (Expr*)linitial(op->args);

            if (list_length(op->args) == 1) {
                isOperatorMOTReady = IsMOTExpr(baserel, state, marr, l, &l, setLocal);
                break;
            }

            Expr* r = (Expr*)lsecond(op->args);
            isOperatorMOTReady = IsMOTExpr(baserel, state, marr, l, &l, setLocal);
            isOperatorMOTReady &= IsMOTExpr(baserel, state, marr, r, &r, setLocal);

            // handles case when column = column|const <oper> column|const
            if (result != nullptr && isOperatorMOTReady) {
                if (IsA(l, Var) && IsA(r, Var) && ((Var*)l)->varno == ((Var*)r)->varno)
                    isOperatorMOTReady = false;
                break;
            }

            isOperatorMOTReady &= GetKeyOperation(op, oper);
            if (isOperatorMOTReady && marr != nullptr) {
                Var* v = nullptr;
                Expr* e = nullptr;

                // this covers case when baserel.a = t2.a <==> t2.a = baserel.a both will be of type Var
                // we have to choose as Expr t2.a cause it will be replaced later with a Param type
                if (IsA(l, Var)) {
                    if (!IsA(r, Var)) {
                        if (IsSameRelation(r, ((Var*)l)->varno)) {
                            isOperatorMOTReady = false;
                            break;
                        }
                        v = (Var*)l;
                        e = r;
                    } else {
                        if (((Var*)l)->varno == ((Var*)r)->varno) {  // same relation
                            return false;
                        } else if (bms_is_member(((Var*)l)->varno, baserel->relids)) {
                            v = (Var*)l;
                            e = r;
                        } else {
                            v = (Var*)r;
                            e = l;
                            RevertKeyOperation(oper);
                        }
                    }
                } else if (IsA(r, Var)) {
                    if (IsSameRelation(l, ((Var*)r)->varno)) {
                        isOperatorMOTReady = false;
                        break;
                    }
                    v = (Var*)r;
                    e = l;
                    RevertKeyOperation(oper);
                } else {
                    isOperatorMOTReady = false;
                    break;
                }

                if (oper == KEY_OPER::READ_KEY_LIKE) {
                    if (!IsA(e, Const))
                        return false;

                    // we support only prefix search: 'abc%' or 'abc', the last transforms into equal
                    Const* c = (Const*)e;
                    if (DatumGetPointer(c->constvalue) == NULL)
                        return false;

                    int len = 0;
                    char* s = DatumGetPointer(c->constvalue);
                    int i = 0;

                    if (c->constlen > 0)
                        len = c->constlen;
                    else if (c->constlen == -1) {
                        struct varlena* vs = (struct varlena*)DatumGetPointer(c->constvalue);
                        s = VARDATA(c->constvalue);
                        len = VARSIZE_ANY(vs) - VARHDRSZ;
                    } else if (c->constlen == -2) {
                        len = strlen(s);
                    }

                    for (; i < len; i++) {
                        if (s[i] == '%')
                            break;

                        if (s[i] == '_')  // we do not support single char pattern
                            return false;
                    }

                    if (i < len - 1)
                        return false;
                }
                isOperatorMOTReady = MOTAdaptor::SetMatchingExpr(state, marr, v->varoattno, oper, e, expr, setLocal);
            }
            break;
        }
        case T_FuncExpr: {
            FuncExpr* func = (FuncExpr*)expr;

            if (func->funcformat == COERCE_IMPLICIT_CAST || func->funcformat == COERCE_EXPLICIT_CAST) {
                isOperatorMOTReady = IsMOTExpr(baserel, state, marr, (Expr*)linitial(func->args), result, setLocal);
            } else if (list_length(func->args) == 0) {
                isOperatorMOTReady = true;
            }

            break;
        }
        case T_RelabelType: {
            isOperatorMOTReady = IsMOTExpr(baserel, state, marr, ((RelabelType*)expr)->arg, result, setLocal);
            break;
        }
        default: {
            isOperatorMOTReady = false;
            break;
        }
    }

    return isOperatorMOTReady;
}

uint16_t MOTTimestampToStr(uintptr_t src, char* destBuf, size_t len)
{
    char* tmp = nullptr;
    Timestamp timestamp = DatumGetTimestamp(src);
    tmp = DatumGetCString(DirectFunctionCall1(timestamp_out, timestamp));
    errno_t erc = snprintf_s(destBuf, len, len - 1, tmp);
    pfree_ext(tmp);
    securec_check_ss(erc, "\0", "\0");
    return erc;
}

uint16_t MOTTimestampTzToStr(uintptr_t src, char* destBuf, size_t len)
{
    char* tmp = nullptr;
    TimestampTz timestamp = DatumGetTimestampTz(src);
    tmp = DatumGetCString(DirectFunctionCall1(timestamptz_out, timestamp));
    errno_t erc = snprintf_s(destBuf, len, len - 1, tmp);
    pfree_ext(tmp);
    securec_check_ss(erc, "\0", "\0");
    return erc;
}

uint16_t MOTDateToStr(uintptr_t src, char* destBuf, size_t len)
{
    char* tmp = nullptr;
    DateADT date = DatumGetDateADT(src);
    tmp = DatumGetCString(DirectFunctionCall1(date_out, date));
    errno_t erc = snprintf_s(destBuf, len, len - 1, tmp);
    pfree_ext(tmp);
    securec_check_ss(erc, "\0", "\0");
    return erc;
}
