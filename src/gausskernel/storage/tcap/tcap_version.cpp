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
 * tcap_version.cpp
 *      Routines to support Timecapsule `Version-based query, restore`.
 *      We use Tv prefix to indicate it in following coding.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/tcap/tcap_version.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/pg_snapshot.h"
#include "commands/tablecmds.h"
#include "executor/node/nodeModifyTable.h"
#include "fmgr.h"
#include "nodes/plannodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "postmaster/snapcapturer.h"
#include "rewrite/rewriteManip.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

#include "storage/tcap.h"
#include "catalog/pg_constraint.h"

static bool TvIsContainsForeignKey(Oid relid)
{
    Relation rbRel;
    SysScanDesc sd;
    ScanKeyData key;
    HeapTuple tup;
    bool isContainsForeignKey = false;

    rbRel = heap_open(ConstraintRelationId, AccessShareLock);

    ScanKeyInit(&key, Anum_pg_constraint_conrelid, BTEqualStrategyNumber,
        F_OIDEQ, ObjectIdGetDatum(relid));

    sd = systable_beginscan(rbRel, ConstraintRelidIndexId, true, SnapshotNow, 1, &key);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tup);

        /* Contains a foreign key or referenced by foreign key */
        if (con->contype == CONSTRAINT_FOREIGN && con->conrelid == relid) {
            isContainsForeignKey = true;
            break;
        }
    }

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return isContainsForeignKey;
}

static bool TvIsReferencedByForeignKey(Oid relid)
{
    Relation rbRel;
    SysScanDesc sd;
    HeapTuple tup;
    bool isReferencedByForeignKey = false; 

    rbRel = heap_open(ConstraintRelationId, AccessShareLock);

    sd = systable_beginscan(rbRel, InvalidOid, false, SnapshotNow, 0, NULL);

    while ((tup = systable_getnext(sd)) != NULL) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(tup);

        /* Not referenced by foreign key */
        if (con->confrelid  == relid) {
            isReferencedByForeignKey = true;
            break;
        }
    }

    systable_endscan(sd);
    heap_close(rbRel, AccessShareLock);

    return isReferencedByForeignKey;
}

static bool TvForeignKeyCheck(Oid relid)
{
    return (TvIsContainsForeignKey(relid) || TvIsReferencedByForeignKey(relid));
}

static bool TvFeatureSupport(Oid relid, char **errstr, bool isTimecapsuleTable)
{
    Relation rel = RelationIdGetRelation(relid);
    Form_pg_class classForm;

    if (!RelationIsValid(rel)) {
        ereport(
            ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR), 
                errmsg("could not open relation with OID %u", relid)));
    }

    classForm = rel->rd_rel;
    if (classForm->relkind != RELKIND_RELATION) {
        *errstr = "timecapsule feature does not support non-ordinary table";
    } else if (is_sys_table(RelationGetRelid(rel))) {
        *errstr = "timecapsule feature does not support system table";
    } else if (classForm->relpersistence != RELPERSISTENCE_PERMANENT) {
        *errstr = "timecapsule feature does not support non-permanent table";
    } else if (classForm->parttype != PARTTYPE_NON_PARTITIONED_RELATION) {
        *errstr = "timecapsule feature does not support partitioned table";
    } else if ((RELATION_HAS_BUCKET(rel) || RELATION_OWN_BUCKET(rel))) {
        *errstr = "timecapsule feature does not support hash-bucket table";
    } else if (!RelationIsRowFormat(rel)) {
        *errstr = "timecapsule feature does not support non-row oriented table";
    } else if (classForm->relisshared) {
        *errstr = "timecapsule feature does not support shared table";
    } else if (classForm->relhassubclass) {
        *errstr = "timecapsule feature does not support derived table";
    } else if (classForm->relhasclusterkey) {
        *errstr = "timecapsule feature does not support table with PARTIAL CLUSTER KEY constraint";
    } else if (RelationInClusterResizing(rel)) {
        *errstr = "timecapsule feature does not support when resizing table";
    } else if (g_instance.role != VSINGLENODE) {
        *errstr = "timecapsule feature does not support in non-singlenode mode";
    } else if (IsolationUsesXactSnapshot()) {
        *errstr = "timecapsule feature does not support in non READ COMMITTED transaction";
    } else if (TvForeignKeyCheck(relid) && isTimecapsuleTable) {
        *errstr = "timecapsule feature does not support the table included foreign key or referenced by foreign key";
    } else {
        *errstr = NULL;
    }

    RelationClose(rel);

    return *errstr == NULL;
}


void TvCheckVersionScan(RangeTblEntry *rte)
{
    char *errstr = NULL;

    if (!TvFeatureSupport(rte->relid, &errstr, false)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                (errmsg("%s", errstr)))); 
    }

    return;
}

bool TvIsVersionScan(const ScanState *ss)
{
    EState *estate = ss->ps.state;
    Scan *scan = (Scan *)ss->ps.plan;
    TimeCapsuleClause *tcc = rt_fetch(scan->scanrelid, estate->es_range_table)->timecapsule;

    return tcc != NULL;
}

/*
 * Whether the plan contains version table scan.
 */
bool TvIsVersionPlan(const PlannedStmt *stmt)
{
    ListCell *l = NULL;
    foreach (l, stmt->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(l);

        if (rte->timecapsule != NULL) {
            return true;
        }
    }

    return false;
}

Node *TvTransformVersionExpr(ParseState *pstate, TvVersionType tvtype, Node *tvver)
{
    Node *verExpr = tvver;

    verExpr = transformExpr(pstate, tvver);
    if (checkExprHasSubLink(verExpr)) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OPERATION),
                errmsg("timecapsule clause not support sublink.")));
    }
    
    if (tvtype == TV_VERSION_TIMESTAMP) {
        verExpr = coerce_to_specific_type(pstate, verExpr, TIMESTAMPTZOID, "TIMESTAMP");
    } else {
        verExpr = coerce_to_specific_type(pstate, verExpr, INT8OID, "CSN");
    }
    assign_expr_collations(pstate, verExpr);

    return verExpr;
}


static Const *TvEvalVerExpr(TvVersionType tvtype, Node *tvver)
{
    Const *result = (Const *)tvver;

    if (!IsA(result, Const)) {
        Node *verExpr;
        ParseState *pstate = make_parsestate(NULL);

        verExpr = TvTransformVersionExpr(pstate, tvtype, tvver);
        free_parsestate(pstate);

        result = (Const *)evaluate_expr((Expr *)verExpr, exprType(verExpr), 
            exprTypmod(verExpr), exprCollation(verExpr));
    }

    if (!IsA(result, Const) || result->constisnull) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("timecapsule clause is not constant expression or evaulted null value.")));
    }

    return result;
}

/*
 * We use the round-down way to obtain snapshots. that is,
 *     select * from pg_snapshot where snptime <= :tz order by snptime desc limit 1;
 */
static void TvFetchSnapTz(TimestampTz tz, Snapshot snap)
{
    Relation rel;
    SysScanDesc sd;
    ScanKeyData skey[3];
    HeapTuple tup;
    Datum value;
    bool isnull = false;
    char *snapstr = NULL;

    if (timestamptz_cmp_internal(tz, GetCurrentTimestamp()) > 0) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
            errmsg("invalid timestamp specified")));
    }

    rel = heap_open(SnapshotRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_snapshot_snptime, BTLessEqualStrategyNumber, 
        F_TIMESTAMP_LE, TimestampTzGetDatum(tz));

    sd = systable_beginscan(rel, SnapshotTimeCsnIndexId, true, NULL, 1, skey);
    tup = systable_getnext(sd);
    /* Limit 1 */
    if (tup == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
            errmsg("cannot find the restore point")));
    }

    value = heap_getattr(tup, Anum_pg_snapshot_snpsnapshot, RelationGetDescr(rel), &isnull);
    snapstr = TextDatumGetCString(value);

    TxnSnapDeserialize(snapstr, snap);

    systable_endscan(sd);
    heap_close(rel, AccessShareLock);

    pfree_ext(snapstr);

    return;
}


/*
 * We use the round-down way to obtain snapshots. that is,
 *     select * from pg_snapshot where snpcsn <= :csn order by snpcsn desc limit 1;
 */
static void TvFetchSnapCsn(int64 csn, Snapshot snap)
{
    Relation rel;
    SysScanDesc sd;
    ScanKeyData skey[3];
    HeapTuple tup;
    Datum value;
    bool isnull = false;
    char *snapstr = NULL;

    if (csn < 0 || (CommitSeqNo)csn > t_thrd.xact_cxt.ShmemVariableCache->nextCommitSeqNo) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
            errmsg("invalid csn specified")));
    }

    rel = heap_open(SnapshotRelationId, AccessShareLock);

    ScanKeyInit(&skey[0], Anum_pg_snapshot_snpcsn, BTLessEqualStrategyNumber, 
        F_INT8LE, Int64GetDatum(csn));

    sd = systable_beginscan(rel, SnapshotCsnXminIndexId, true, NULL, 1, skey);

    tup = systable_getnext(sd);
    if (tup == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
            errmsg("restore point not found")));
    }

    /* Limit 1 */
    value = heap_getattr(tup, Anum_pg_snapshot_snpsnapshot, RelationGetDescr(rel), &isnull);
    snapstr = TextDatumGetCString(value);

    TxnSnapDeserialize(snapstr, snap);

    /*
     * Use user-specified CSN if not exactly snapshot matched.
     *
     * Notice: Inexact user-specified CSN expands the scope of earlier versions 
     * that must be checked for visibility. The round-down way enables 
     * the snap.xmin as the left boundary of visibility check. However, snap.xmax 
     * must be set to MaxTransactionId to ensure the correctness.
     */
    if ((CommitSeqNo)csn != snap->snapshotcsn) {
        snap->snapshotcsn = (CommitSeqNo)csn;
        snap->timeline = 0;
        snap->xmin = snap->xmin;
        snap->xmax = MaxTransactionId;
    }

    systable_endscan(sd);
    heap_close(rel, AccessShareLock);

    pfree_ext(snapstr);

    return;
}

static Snapshot TvFetchSnap(TvVersionType type, Const *value)
{
    Snapshot snap = (Snapshot)palloc0(sizeof(SnapshotData));

    if (type == TV_VERSION_TIMESTAMP) {
        TvFetchSnapTz(DatumGetTimestampTz(value->constvalue), snap);
    } else {
        TvFetchSnapCsn(DatumGetInt64(value->constvalue), snap);
    }

    snap->satisfies = SNAPSHOT_VERSION_MVCC;

    return snap;
}

static Snapshot TvGetSnap(Relation relation, TvVersionType tvtype, Node *tvver)
{
    Const *value;
    Snapshot snap;

    value = TvEvalVerExpr(tvtype, tvver);

    snap = TvFetchSnap(tvtype, value);

    if (!tableam_tcap_validate_snap(relation, snap)) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
            errmsg("Restore point too old")));
    }

    return snap;
}

static void TvValidateRelDDL(Oid relid, CommitSeqNo snapcsn)
{
    Relation rel = RelationIdGetRelation(relid);
    if (!RelationIsValid(rel)) {
        ereport(
            ERROR, (errcode(ERRCODE_RELATION_OPEN_ERROR), 
                errmsg("could not open relation with OID %u", relid)));
    }

    if (RelationGetChangecsn(rel) >= snapcsn) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
            errmsg("The table definition of \"%s\" has been changed.", 
                RelationGetRelationName(rel))));
    }

    RelationClose(rel);

    return;
}

/*
 * Choose user-specified snapshot if TimeCapsule clause exists, otherwise 
 * estate->es_snapshot instead.
 */
Snapshot TvChooseScanSnap(Relation relation, Scan *scan, ScanState *ss)
{
    EState *estate = ss->ps.state;
    Snapshot snap = estate->es_snapshot;
    RangeTblEntry *rte = rt_fetch(scan->scanrelid, estate->es_range_table);
    TimeCapsuleClause *tcc = rte->timecapsule;

    if (likely(tcc == NULL)) {        
        return snap;    
    } else {
        bool isnull = false;
        ExprContext *econtext;
        Datum val;
        Const *con;

        econtext = CreateExprContext(estate);
        val = ExecEvalExprSwitchContext(ExecInitExpr((Expr *)tcc->tvver, &ss->ps), 
            econtext, &isnull, NULL);
        con = makeConst((tcc->tvtype == TV_VERSION_TIMESTAMP) ? TIMESTAMPTZOID : INT8OID, 
            -1, InvalidOid, 8, val, isnull, true);

        snap = TvGetSnap(relation, tcc->tvtype, (Node *)con);
        TvValidateRelDDL(rte->relid, snap->snapshotcsn);

        FreeExprContext(econtext, true);
        pfree(con);
    }

    return snap;    
}

void TvDeleteDelta(Oid relid, Snapshot snap)
{
    Relation rel;
    TableScanDesc sd;
    HeapTuple tup;

    /* Notice: invoker already acquired lock */
    rel = heap_open(relid, NoLock);

    sd = tableam_scan_begin(rel, snap, 0, NULL);
    while ((tup = (HeapTuple)tableam_scan_getnexttuple(sd, ForwardScanDirection)) != NULL) {
        simple_heap_delete(rel, &tup->t_self);
    }

    tableam_scan_end(sd);
    heap_close(rel, NoLock);
    return;
}

void TvUheapDeleteDelta(Oid relid, Snapshot snap)
{
    Relation rel;
    TableScanDesc sd;
    UHeapTuple tup;
    TupleTableSlot *oldslot = NULL;

    Snapshot snapshotNow = (Snapshot)palloc0(sizeof(SnapshotData));
    (void)GetSnapshotData(snapshotNow, false);
    snap->user_data = (void *)snapshotNow;

    /* Notice: invoker already acquired lock */
    rel = heap_open(relid, NoLock);

    EState *estate = CreateExecutorState();
    /*
     * We need a ResultRelInfo so we can use the regular executor's
     * index-entry-making machinery.  (There used to be a huge amount of code
     * here that basically duplicated execUtils.c ...)
     */
    ResultRelInfo *resultRelInfo = makeNode(ResultRelInfo);
    resultRelInfo->ri_RangeTableIndex = 1;      /* dummy */
    resultRelInfo->ri_RelationDesc = rel;
    ExecOpenIndices(resultRelInfo, false);
    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;

    sd = tableam_scan_begin(rel, snap, 0, NULL);
    while ((tup = (UHeapTuple)tableam_scan_getnexttuple(sd, ForwardScanDirection)) != NULL) {
        SimpleUHeapDelete(rel, &tup->ctid, snapshotNow, &oldslot);
        ExecDeleteIndexTuples(oldslot, &tup->ctid, estate, rel, NULL, NULL, false);
        if (oldslot) {
            ExecDropSingleTupleTableSlot(oldslot);
            oldslot = NULL;
        }
    }
    tableam_scan_end(sd);
    heap_close(rel, NoLock);

    FreeSnapshotDeepForce(snapshotNow);
    snap->user_data = NULL;

    ExecCloseIndices(resultRelInfo);
    pfree(resultRelInfo);

    return;
}

typedef HeapTuple (*TvFetchTupleHook)(void *arg);
static HeapTuple TvFetchTuple(void *arg)
{
    HeapTuple tup = (HeapTuple)tableam_scan_getnexttuple((TableScanDesc)arg, ForwardScanDirection);

    return tup ? (HeapTuple)tableam_tops_copy_tuple(tup) : NULL;
}

typedef UHeapTuple (*TvUheapFetchTupleHook)(void *arg);
static UHeapTuple TvUheapFetchTuple(void *arg)
{
    return (UHeapTuple)tableam_scan_getnexttuple((TableScanDesc)arg, ForwardScanDirection);
}

static void TvBatchInsert(Relation rel, EState *estate, CommandId mycid,
                    int hiOptions, ResultRelInfo *resultRelInfo,
                    TupleTableSlot *myslot, BulkInsertState bistate,
                    int nBufferedTuples, HeapTuple *bufferedTuples)
{
    MemoryContext oldcontext;
    HeapMultiInsertExtraArgs args = {NULL, 0, false};

    /*
     * heap_multi_insert leaks memory, so switch to short-lived memory context
     * before calling it.
     */
    oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
    (void)tableam_tuple_multi_insert(rel, resultRelInfo->ri_RelationDesc, (Tuple *)bufferedTuples, nBufferedTuples,
        mycid, hiOptions, bistate, &args);
    MemoryContextSwitchTo(oldcontext);

    /*
     * If there are any indexes, update them for all the inserted tuples.
     */
    if (resultRelInfo->ri_NumIndices > 0) {
        int i;
        for (i = 0; i < nBufferedTuples; i++) {
            List *recheckIndexes = NULL;

            (void)ExecStoreTuple(bufferedTuples[i], myslot, InvalidBuffer, false);

            recheckIndexes = ExecInsertIndexTuples(myslot,
                &(bufferedTuples[i]->t_self),
                estate,
                NULL,
                NULL,
                InvalidBktId,
                NULL,
                NULL);

            list_free(recheckIndexes);
        }
    }

    return;
}

const int MAX_BUFFERED_TUPLES_TCAP = 1000;
const int MAX_BUFFERED_TUPLES_NUM_TCAP = 65535;
static void TvInsertLostImpl(Relation rel, Snapshot snap, TvFetchTupleHook fetchTupleHook, void *arg)
{
    HeapTuple tuple;
    ResultRelInfo *resultRelInfo;
    EState *estate = CreateExecutorState();
    TupleTableSlot *myslot = NULL;
    MemoryContext oldcontext = CurrentMemoryContext;
    CommandId mycid = GetCurrentCommandId(true);
    int hiOptions = 0;
    BulkInsertState bistate;
    int nBufferedTuples = 0;

    HeapTuple *bufferedTuples = NULL;
    Size bufferedTuplesSize = 0;

    /*
     * We need a ResultRelInfo so we can use the regular executor's
     * index-entry-making machinery.  (There used to be a huge amount of code
     * here that basically duplicated execUtils.c ...)
     */
    resultRelInfo = makeNode(ResultRelInfo);
    resultRelInfo->ri_RangeTableIndex = 1;      /* dummy */
    resultRelInfo->ri_RelationDesc = rel;

    ExecOpenIndices(resultRelInfo, false);

    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;

    /* Set up a tuple slot too */
    myslot = ExecInitExtraTupleSlot(estate);
    ExecSetSlotDescriptor(myslot, RelationGetDescr(rel));

    bufferedTuples = (HeapTuple *)palloc0(MAX_BUFFERED_TUPLES_TCAP * sizeof(HeapTuple));

    bistate = GetBulkInsertState();

    for (;;) {
        TupleTableSlot *slot = NULL;

        CHECK_FOR_INTERRUPTS();
        if (nBufferedTuples == 0) {
            /*
             * Reset the per-tuple exprcontext. We can only do this if the
             * tuple buffer is empty (calling the context the per-tuple memory
             * context is a bit of a misnomer now
             */
            ResetPerTupleExprContext(estate);
        }

        /* Switch into its memory context */
        MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

        if ((tuple = fetchTupleHook(arg)) == NULL) {
            break;
        }

        slot = myslot;
        ExecStoreTuple(tuple, slot, InvalidBuffer, false);

        /* Check the constraints of the tuple */
        if (rel->rd_att->constr) {
            ExecConstraints(resultRelInfo, slot, estate);
        }

        bufferedTuples[nBufferedTuples++] = tuple;
        bufferedTuplesSize += tuple->t_len;

        /*
         * If the buffer filled up, flush it. Also flush if the total
         * size of all the tuples in the buffer becomes large, to
         * avoid using large amounts of memory for the buffers when
         * the tuples are exceptionally wide.
         */
        if (nBufferedTuples == MAX_BUFFERED_TUPLES_TCAP || bufferedTuplesSize > MAX_BUFFERED_TUPLES_NUM_TCAP) {
            TvBatchInsert(rel, estate, mycid, hiOptions, resultRelInfo, myslot, bistate, nBufferedTuples, 
                bufferedTuples);

            nBufferedTuples = 0;
            bufferedTuplesSize = 0;
        }
    }

    /* Flush any remaining buffered tuples */
    if (nBufferedTuples > 0) {
        TvBatchInsert(rel, estate, mycid, hiOptions, resultRelInfo, myslot, bistate, nBufferedTuples, bufferedTuples);
    }

    FreeBulkInsertState(bistate);
    
    MemoryContextSwitchTo(oldcontext);

    ExecResetTupleTable(estate->es_tupleTable, false);

    ExecCloseIndices(resultRelInfo);

    FreeExecutorState(estate);

    pfree(bufferedTuples);
    pfree(resultRelInfo);

    return;
}

static void TvUheapInsertLostImpl(Relation rel, Snapshot snap, TvUheapFetchTupleHook fetchTupleHook, void *arg)
{
    UHeapTuple tuple;
    ResultRelInfo *resultRelInfo;
    EState *estate = CreateExecutorState();
    TupleTableSlot *myslot;
    CommandId mycid = GetCurrentCommandId(true);
    /*
     * We need a ResultRelInfo so we can use the regular executor's
     * index-entry-making machinery.  (There used to be a huge amount of code
     * here that basically duplicated execUtils.c ...)
     */
    resultRelInfo = makeNode(ResultRelInfo);
    resultRelInfo->ri_RangeTableIndex = 1;      /* dummy */
    resultRelInfo->ri_RelationDesc = rel;
    ExecOpenIndices(resultRelInfo, false);
    estate->es_result_relations = resultRelInfo;
    estate->es_num_result_relations = 1;
    estate->es_result_relation_info = resultRelInfo;

    /* Set up a tuple slot too */
    myslot = ExecInitExtraTupleSlot(estate, TAM_USTORE);
    ExecSetSlotDescriptor(myslot, RelationGetDescr(rel));

    /* Switch into its memory context */
    MemoryContext oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
    for (;;) {
        TupleTableSlot *slot = NULL;

        CHECK_FOR_INTERRUPTS();
        if ((tuple = fetchTupleHook(arg)) == NULL) {
            break;
        }

        slot = myslot;
        ExecStoreTuple(tuple, slot, InvalidBuffer, false);

        /* Check the constraints of the tuple */
        if (rel->rd_att->constr) {
            ExecConstraints(resultRelInfo, slot, estate);
        }
        UHeapInsert(rel, tuple, mycid, NULL);

        List *recheckIndexes = NULL;
        recheckIndexes = ExecInsertIndexTuples(myslot, &tuple->ctid, estate, NULL, NULL, InvalidBktId, NULL, NULL);
        list_free(recheckIndexes);
    }
    MemoryContextSwitchTo(oldcontext);

    ExecResetTupleTable(estate->es_tupleTable, false);

    ExecCloseIndices(resultRelInfo);

    FreeExecutorState(estate);

    pfree(resultRelInfo);

    return;
}

void TvInsertLost(Oid relid, Snapshot snap)
{
    Relation rel;
    TableScanDesc sd;

    /* 1. Prepare to fetch lost tuples. */
    rel = heap_open(relid, NoLock);
    sd = tableam_scan_begin(rel, snap, 0, NULL);

    /* 2. Multi insert. */
    TvInsertLostImpl(rel, snap, TvFetchTuple, (void *)sd);

    /* 3. Done, clean. */
    tableam_scan_end(sd);
    heap_close(rel, NoLock);
    return;
}

void TvUheapInsertLost(Oid relid, Snapshot snap)
{
    Relation rel;
    TableScanDesc sd;
    /* 1. Prepare to fetch lost tuples. */
    rel = heap_open(relid, NoLock);
    sd = tableam_scan_begin(rel, snap, 0, NULL);
    /* 2. Insert one by one. */
    TvUheapInsertLostImpl(rel, snap, TvUheapFetchTuple, (void *)sd);
    /* 3. Done, clean. */
    tableam_scan_end(sd);
    heap_close(rel, NoLock);
    return;
}

static void TvCheckVersionRestore(Relation rel)
{
    char *errstr = NULL;

    /*
     * 1. Check whether restore version allowed.
     */
    if (!TvFeatureSupport(RelationGetRelid(rel), &errstr, true)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                (errmsg("%s", errstr)))); 
    }

    /*
     * 2. Permissions check, only the owner of the table can do this operation.
     */
    if (!pg_class_ownercheck(RelationGetRelid(rel), GetUserId())) {
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS, NameStr(rel->rd_rel->relname));
    }

    return;
}

void TvRestoreVersion(TimeCapsuleStmt *stmt)
{
    Relation rel;
    Snapshot snap;

    /*
     * 1. Lock relation, and check admission.
     */
    rel = heap_openrv(stmt->relation, AccessExclusiveLock);
    TvCheckVersionRestore(rel);

    /*
     * 2. Get restore point snapshot.
     */
    snap = TvGetSnap(rel, stmt->tvtype, stmt->tvver);
    TvValidateRelDDL(RelationGetRelid(rel), snap->snapshotcsn);

    /*
     * 3. Delete delta tuples that inserted after restore point.
     */
    snap->satisfies = SNAPSHOT_DELTA;
    tableam_tcap_delete_delta(rel, snap);

    /*
     * 4. Insert lost tuples that inserted before restore point 
     *    and deleted after restore point.
     */
    snap->satisfies = SNAPSHOT_LOST;
    tableam_tcap_insert_lost(rel, snap);

    /*
     * 5. We are done.
     */
    pfree_ext(snap);
    heap_close(rel, NoLock);

    /*
     * CommandCounterIncrement here to ensure that preceding changes are all
     * visible to the next command.
     */
    CommandCounterIncrement();

    return;
}

