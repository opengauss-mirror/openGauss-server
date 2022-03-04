/* -------------------------------------------------------------------------
 *
 * matview.c
 * materialized view support
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/backend/commands/matview.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/tableam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/pg_opclass.h"
#include "catalog/pgxc_class.h"
#include "catalog/index.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/gs_matview.h"
#include "catalog/gs_matview_dependency.h"
#include "catalog/toasting.h"
#include "catalog/cstore_ctlg.h"
#include "commands/cluster.h"
#include "commands/matview.h"
#include "commands/createas.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "parser/parse_hint.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lmgr.h"
#include "storage/smgr/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/snapmgr.h"
#include "nodes/parsenodes.h"
#include "optimizer/nodegroups.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"

#define DatumGetItemPointer(X) ((ItemPointer)DatumGetPointer(X))
#define ItemPointerGetDatum(X) PointerGetDatum(X)

typedef struct matview_shmem_t {
    struct LWLock* matview_lck;
    TransactionId matview_xmin;
    int64 seqno;
    bool seqno_invalid;
} matview_shmem_t;

matview_shmem_t *matview_shmem;

typedef struct {
    DestReceiver pub; /* publicly-known function pointers */
    IntoClause* into; /* target relation specification */
    Query* viewParse; /* the query which defines/populates data */
    /* These fields are filled by intorel_startup: */
    Relation rel;            /* relation to write to */
    CommandId output_cid;    /* cmin to insert in output tuples */
    int hi_options;          /* heap_insert performance options */
    BulkInsertState bistate; /* bulk insert state */
} DR_intorel;

typedef struct {
   DestReceiver pub;           /* publicly-known function pointers */
   Oid         transientoid;   /* OID of new heap into which to store */
   /* These fields are filled by transientrel_startup: */
   Relation    transientrel;   /* relation to write to */
   CommandId   output_cid;     /* cmin to insert in output tuples */
   int         hi_options;     /* heap_insert performance options */
   BulkInsertState bistate;    /* bulk insert state */
} DR_transientrel;

typedef struct {
    AttrNumber attnum;
    Oid atttype;
    int32 atttypmod;
} DistKeyInfo;

typedef bool (*RTEChecker)(const RangeTblEntry *);

static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void transientrel_receive(TupleTableSlot *slot, DestReceiver *self);
static void transientrel_shutdown(DestReceiver *self);
static void transientrel_destroy(DestReceiver *self);

static void refresh_matview_datafill(DestReceiver *dest, Query *query,
                                    const char *queryString);
static Oid find_mlog_table(Oid relid);
static void update_mlog_time(Relation mlog, HeapTuple tuple, Datum curtime, CatalogIndexState indstate);
static void check_simple_query(Query *query, List **distkeyList, List **rangeTables, Oid *groupid);
static void check_union_all(Query *query, List **distkeyList, List **rangeTables, Oid *groupid);
static void check_set_op_component(Node *node);
static void check_table(RangeTblEntry *rte, Oid *groupid);
static List* get_distkey_info(Oid oid);
static void update_mlog_time_all(Oid mlogid, Datum curtime);
static List* get_rest_index_ref(QueryDesc* queryDesc, Oid relid);
static void vacuum_mlog_for_matview(Oid matviewid);
static void vacuum_mlog(Oid mlogid);
static void MlogClearLogByTime(Oid mlogID, Timestamp ts);
static void ExecCreateMatInc(QueryDesc*queryDesc, Query *query, Relation matview,
                                    Oid mapid, Datum curtime);
static bool BasetableWalker(Node *node, List** rteList);
static void clearup_matviewmap_tuple(Oid mapid);
static void ExecHandleIncIndex(TupleTableSlot *slot, Relation matview, HeapTuple copyTuple);

static int64 MlogGetSeqNo();
static int64 MlogGetMaxSeqno(Oid mlogid);
static int64 MlogComputeSeqNo();

/*
 * Shared memory size
 */
Size MatviewShmemSize(void)
{
    return sizeof(matview_shmem_t);
}

inline PlannedStmt* GetPlanStmt(Query* query, ParamListInfo params)
{
    PlannedStmt* plan;

    int outerdop = u_sess->opt_cxt.query_dop;
    bool enable_indexonlyscan = u_sess->attr.attr_sql.enable_indexonlyscan;

    PG_TRY();
    {
        u_sess->opt_cxt.query_dop = 1;
        u_sess->attr.attr_sql.enable_indexonlyscan = false;
        RemoveQueryHintByType(query, HINT_KEYWORD_INDEXONLYSCAN);

        plan = pg_plan_query(query, 0, params);

        u_sess->opt_cxt.query_dop = outerdop;
        u_sess->attr.attr_sql.enable_indexonlyscan = enable_indexonlyscan;
    }
    PG_CATCH();
    {
        u_sess->opt_cxt.query_dop = outerdop;
        u_sess->attr.attr_sql.enable_indexonlyscan = enable_indexonlyscan;
        PG_RE_THROW();
    }
    PG_END_TRY();

    return plan;
}

/*
 * Initialize shared memory
 */
void MatviewShmemInit(void)
{
    bool found = false;
    matview_shmem =
        (matview_shmem_t*)ShmemInitStruct("matview shmem", sizeof(matview_shmem_t), &found);
    if (!IsUnderPostmaster) {
        Assert(!found);

        matview_shmem->matview_lck = LWLockAssign(LWTRANCHE_MATVIEW_SEQNO);
        matview_shmem->matview_xmin = InvalidTransactionId;
        matview_shmem->seqno = 0;
        matview_shmem->seqno_invalid = false;
    } else {
        Assert(found);
    }
}

/* get the next seq no. */
int64 MlogGetSeqNo()
{
    int64 seqno = 0;
    if (matview_shmem == NULL) {
        return 0;
    }

    LWLockAcquire(matview_shmem->matview_lck, LW_EXCLUSIVE);
    if (!matview_shmem->seqno_invalid) {
        matview_shmem->seqno = MlogComputeSeqNo();
        matview_shmem->seqno_invalid = true;
    }

    seqno = matview_shmem->seqno;
    matview_shmem->seqno = matview_shmem->seqno + 1;
    LWLockRelease(matview_shmem->matview_lck);

    return seqno;
}

void MatviewShmemSetInvalid()
{
    if (matview_shmem == NULL) {
        return;
    }

    LWLockAcquire(matview_shmem->matview_lck, LW_EXCLUSIVE);
    matview_shmem->seqno_invalid = false;
    LWLockRelease(matview_shmem->matview_lck);

    return;
}

/* get the max(seqno) + 1 which is in the mlog */
int64 MlogComputeSeqNo()
{
    int64 result = 0;
    int64 seqno = 0;
    HeapTuple tup;
    TableScanDesc scan;
    Relation matview_dep;
    Form_gs_matview_dependency matviewdepForm;

    matview_dep = heap_open(MatviewDependencyId, AccessShareLock);
    scan = tableam_scan_begin(matview_dep, SnapshotNow, 0, NULL);
    tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while(tup != NULL) {
        matviewdepForm = (Form_gs_matview_dependency)GETSTRUCT(tup);

        Relation base_rel = try_relation_open(matviewdepForm->relid, AccessShareLock);
        if (base_rel == NULL) {
            tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }

        Relation mat_rel = try_relation_open(matviewdepForm->matviewid, AccessShareLock);
        if (mat_rel == NULL) {
            heap_close(base_rel, AccessShareLock);
            tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
            continue;
        }

        Oid mlogid = matviewdepForm->mlogid;
        seqno = MlogGetMaxSeqno(mlogid);

        heap_close(mat_rel, AccessShareLock);
        heap_close(base_rel, AccessShareLock);

        if (result < seqno) {
            result = seqno;
        }

        tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    result++;

    tableam_scan_end(scan);
    heap_close(matview_dep, NoLock);
    return result;
}

int64 MlogGetMaxSeqno(Oid mlogid)
{
    int64 result = 0;
    bool isNull = false;
    HeapTuple tuple = NULL;
    IndexScanDesc indexScan;
    Relation mlog = heap_open(mlogid, AccessShareLock);

    List *indexoidlist = RelationGetIndexList(mlog);
    if (indexoidlist == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can't find index on %s", RelationGetRelationName(mlog))));
    }
    Oid indexoid = (Oid)linitial_oid(indexoidlist);

    Relation mlogidx = index_open(indexoid, AccessShareLock);

    indexScan = index_beginscan(mlog, mlogidx, SnapshotAny, 0, 0);
    index_rescan(indexScan, NULL, 0, NULL, 0);

    if ((tuple = (HeapTuple)index_getnext(indexScan, BackwardScanDirection)) != NULL) {
        Datum num = heap_getattr(tuple,
                                MlogAttributeSeqno,
                                RelationGetDescr(mlog),
                                &isNull);

        if (isNull) {
            ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("seqno should not be NULL when scan mlog table")));
        }
        result = DatumGetInt64(num);
    }

    list_free_ext(indexoidlist);
    index_endscan(indexScan);
    index_close(mlogidx, AccessShareLock);
    heap_close(mlog, AccessShareLock);

    return result;
}

/*
 * SetRelationIsScannable
 *     Make the relation appear scannable.
 *
 * NOTE: This is only implemented for materialized views. The heap starts out
 * in a state that doesn't look scannable, and can only transition from there
 * to scannable, unless a new heap is created.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void SetRelationIsScannable(Relation relation)
{
   Page page;

   Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);
   Assert(relation->rd_isscannable == false);

   RelationOpenSmgr(relation);
   page = (Page)palloc(BLCKSZ);
   PageInit(page, BLCKSZ, 0, true);
   PageSetChecksumInplace(page, 0);
   smgrextend(relation->rd_smgr, MAIN_FORKNUM, 0, (char *) page, true);
   pfree(page);

   smgrimmedsync(relation->rd_smgr, MAIN_FORKNUM);

   RelationCacheInvalidateEntry(relation->rd_id);
}

static Index get_index_ref(QueryDesc* queryDesc, Oid relid)
{
    ListCell *lc = NULL;
    Index result = 1;
    List *rtable = queryDesc->plannedstmt->rtable;

    foreach (lc, rtable) {
        RangeTblEntry *entry = (RangeTblEntry *)lfirst(lc);

        if (entry->relid == relid) {
            return result;
        }
        result++;
    }
    return 0;
}

/*
 * found rest indexes of rtables excluding current input one.
 */
static List* get_rest_index_ref(QueryDesc* queryDesc, Oid relid)
{
    ListCell *lc = NULL;
    int index = 0;
    List *result = NIL;
    List *rtable = queryDesc->plannedstmt->rtable;

    foreach (lc, rtable) {
        RangeTblEntry *entry = (RangeTblEntry *)lfirst(lc);

        index++;
        if (entry->relid == relid || entry->rtekind != RTE_RELATION) {
            continue;
        }

        result = lappend_int(result, index);
    }

    return result;
}

/*
 * handle write epq tuples when create materiazlied view
 * 1 insert tuple into matview.
 * 2 insert dep-tuple into matmap.
 */
static void ExecHandleMatData(TupleTableSlot *slot, Relation matview, Oid mapid,
                                      Oid relid, HeapTuple reltuple, TransactionId xid)
{
    HeapTuple tuple;
    Oid matid = RelationGetRelid(matview);

    if (slot == NULL || slot->tts_isempty) {
        return;
    }

    tuple = ExecFetchSlotTuple(slot);

    if (tuple == NULL) {
        return;
    }

    HeapTuple copy_tuple = heap_copytuple(tuple);

    simple_heap_insert(matview, copy_tuple);

    /* insert index tuple */
    if (matview->rd_rel->relhasindex) {
        ExecHandleIncIndex(slot, matview, copy_tuple);
    }

    insert_into_matview_map(mapid, matid, &copy_tuple->t_self, relid, &reltuple->t_self, xid);
    heap_freetuple_ext(copy_tuple);
    return;
}

/*
 * Insert Index to matview
 */
static void ExecHandleIncIndex(TupleTableSlot *slot, Relation matview, HeapTuple copy_tuple)
{
    List* indexes = NIL;
    EState* estate = CreateExecutorState();
    MemoryContext old_context = MemoryContextSwitchTo(estate->es_query_cxt);

    ResultRelInfo* relinfo = makeNode(ResultRelInfo);

    InitResultRelInfo(relinfo, matview, 0, 0);
    estate->es_result_relation_info = relinfo;
    ExecOpenIndices(relinfo, false);

    indexes = ExecInsertIndexTuples(slot, &(copy_tuple->t_self), estate, NULL, NULL, InvalidBktId, NULL, NULL);

    ExecCloseIndices(relinfo);
    (void)MemoryContextSwitchTo(old_context);

    if (estate != NULL) {
        FreeExecutorState(estate);
    }

    return;
}

/*
 * Insert Epq tuples into matview
 */
static void ExecHandleIncData(TupleTableSlot *slot, Relation matview, Oid mapid,
                              Oid relid, HeapTuple reltuple, TransactionId xid)
{
    HeapTuple tuple;
    Oid mvid = RelationGetRelid(matview);

    if (slot == NULL || slot->tts_isempty) {
        return;
    }

    tuple = ExecFetchSlotTuple(slot);

    if (tuple == NULL) {
        return;
    }

    HeapTuple copy_tuple = heap_copytuple(tuple);

    /* insert normal tuple */
    simple_heap_insert(matview, copy_tuple);

    /* insert index tuple */
    if (matview->rd_rel->relhasindex) {
        ExecHandleIncIndex(slot, matview, copy_tuple);
    }

    insert_into_matview_map(mapid, mvid, &copy_tuple->t_self, relid, &reltuple->t_self, xid);
    heap_freetuple_ext(copy_tuple);
    return;
}

static void ExecutorMatEpqs(EPQState *epqstate, HeapTuple tuple, int relid,
                            Oid mapid, TransactionId xid, Relation matview, int action)
{
    TupleTableSlot *slot;

    /*
     * Run the EPQ query.  We assume it will return at most one tuple.
     */
    slot = EvalPlanQualNext(epqstate);

    if (action == ActionRefreshInc) {
        ExecHandleIncData(slot, matview, mapid, relid, tuple, xid);
    } else if(action == ActionCreateMat) {
        ExecHandleMatData(slot, matview, mapid, relid, tuple, xid);
    }

    return;
}

static void ExecutorMatDelete(Oid matviewid, ItemPointer ctid)
{
    Relation matview = heap_open(matviewid, RowExclusiveLock);

    simple_heap_delete(matview, ctid);

    heap_close(matview, NoLock);

    return;
}

/*
 * refresh matview incremental meet Action 'D'
 * 1. find matview tuple in matmap based on relid, rel_ctid and matviewid
 * 2. delete tuple from matview based on ctid from matmap.
 */
static void ExecutorMatMapDelete(Oid mapid, Oid relid, ItemPointer rel_ctid,
                                Oid matviewid, TransactionId xmin , bool qualsExist)
{
    SysScanDesc scan;
    HeapTuple tuple;
    bool isNull = false;
    ScanKeyData scankey[3];

    ScanKeyInit(&scankey[0], MatMapAttributeMatid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(matviewid));
    ScanKeyInit(&scankey[1], MatMapAttributeRelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
    ScanKeyInit(&scankey[2], MatMapAttributeRelctid, BTEqualStrategyNumber, F_TIDEQ, ItemPointerGetDatum(rel_ctid));

    Relation mapRel = heap_open(mapid, RowExclusiveLock);
    List *indexOidList = RelationGetIndexList(mapRel);
    if (indexOidList == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Can't find index on %s", RelationGetRelationName(mapRel))));
    }
    Oid indexOid = (Oid)linitial_oid(indexOidList);

    scan = systable_beginscan(mapRel, indexOid, true, NULL, 3, scankey);
    if (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum matctid = heap_getattr(tuple,
                                     MatMapAttributeMatctid,
                                     RelationGetDescr(mapRel),
                                     &isNull);

        TransactionId xid = heap_getattr(tuple,
                                        MatMapAttributeRelxid,
                                        RelationGetDescr(mapRel),
                                        &isNull);

        /*
         * Need check xmin
         * xmin stands for 'D' action when xid is 'I' action in mlog table.
         */
        if (TransactionIdEquals(xmin, xid) || TransactionIdEquals(xid, FrozenTransactionId) ||
            TransactionIdEquals(xmin, FrozenTransactionId)) {
            ExecutorMatDelete(matviewid, DatumGetItemPointer(matctid));

            /* delete from matmap */
            simple_heap_delete(mapRel, &tuple->t_self);
            CommandCounterIncrement();
        } else {
            ereport(WARNING,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                    errmsg("Not found xmin %lu of tuple ctid(%d, %d) xid %lu relid %u in matviewmap %s when ExecutorMatMapDelete",
                    xmin,
                    ItemPointerGetBlockNumber(rel_ctid),
                    ItemPointerGetOffsetNumber(rel_ctid),
                    xid,
                    relid,
                    RelationGetRelationName(mapRel))));
        }
    } else {
        int mode = qualsExist ? DEBUG2 : WARNING;
        ereport(mode,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("Not found ctid(%d, %d), xmin is %lu of relid %u in matviewmap %s when ExecutorMatMapDelete",
                ItemPointerGetBlockNumber(rel_ctid),
                ItemPointerGetOffsetNumber(rel_ctid),
                xmin,
                relid,
                RelationGetRelationName(mapRel))));
    }

    systable_endscan(scan);
    list_free_ext(indexOidList);
    heap_close(mapRel, RowExclusiveLock);

    return;
}

static bool mlog_form_tuple(Relation mlog, HeapTuple mlogTup, Relation rel, HeapTuple *tup)
{
    Assert(mlog != NULL);
    Assert(mlogTup != NULL);

    Datum values[mlog->rd_att->natts] = {0};
    bool nulls[mlog->rd_att->natts] = {0};

    heap_deform_tuple(mlogTup, mlog->rd_att, values, nulls);

    int i;
    int j;
    Datum rel_values[rel->rd_att->natts] = {0};
    bool rel_nulls[rel->rd_att->natts] = {0};
    for (i = 0, j = 0; i < rel->rd_att->natts; i++) {
        if (rel->rd_att->attrs[i]->attisdropped) {
            rel_nulls[i] = true;
        } else {
            rel_values[i] = values[MlogAttributeNum + j];
            rel_nulls[i] = nulls[MlogAttributeNum + j];
            j++;
        }
    }

    *tup = heap_form_tuple(rel->rd_att, rel_values, rel_nulls);

    return true;
}

/*
 * For every tuple pullup from mlog-table execute epq.
 */
static void ExecutorMatRelInc(QueryDesc* queryDesc, Index index, Oid mlogid, Oid relid,
                                Datum curtime, Relation matview, Oid mapid, bool qualsExist)
{
    char act;
    Oid indexoid;
    IndexScanDesc indexScan;
    Relation mlog;
    Relation rel;
    Relation mlogidx;
    HeapTuple tuple;
    bool isNull = false;
    bool isCtidNull = false;
    bool isTimeNULL = false;
    bool isActionNULL = false;
    ListCell *lc = NULL;
    EState *estate = queryDesc->estate;
    EPQState *epqstate = NULL;
    Oid mvid = RelationGetRelid(matview);

    epqstate = (EPQState *)palloc0(sizeof(EPQState));
    EvalPlanQualInit(epqstate, estate, queryDesc->plannedstmt->planTree, NIL, 0);
    epqstate->planstate = queryDesc->planstate;

    Datum oldTime = get_matview_refreshtime(mvid, &isTimeNULL);

    /* pull-up rest indexes of rtables to be NULL. */
    List *rest = get_rest_index_ref(queryDesc, relid);

    mlog = heap_open(mlogid, RowExclusiveLock);
    rel = heap_open(relid, AccessShareLock);

    List *indexoidlist = RelationGetIndexList(mlog);
    if (indexoidlist == NULL) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Can't find index on %s", RelationGetRelationName(mlog))));
    }
    indexoid = (Oid)linitial_oid(indexoidlist);

    mlogidx = index_open(indexoid, RowExclusiveLock);
    CatalogIndexState indstate = CatalogOpenIndexes(mlog);

    indexScan = index_beginscan(mlog, mlogidx, GetTransactionSnapshot(), 0, 0);
    index_rescan(indexScan, NULL, 0, NULL, 0);

    while ((tuple = (HeapTuple)index_getnext(indexScan, ForwardScanDirection)) != NULL) {
        Datum refreshTime = heap_getattr(tuple,
                                        MlogAttributeTime,
                                        RelationGetDescr(mlog),
                                        &isNull);

        Datum ctid = heap_getattr(tuple,
                                MlogAttributeCtid,
                                RelationGetDescr(mlog),
                                &isCtidNull);

        Datum action = heap_getattr(tuple,
                                MlogAttributeAction,
                                RelationGetDescr(mlog),
                                &isActionNULL);

        Datum num = heap_getattr(tuple,
                                MlogAttributeSeqno,
                                RelationGetDescr(mlog),
                                &isActionNULL);

        TransactionId xid = DatumGetTransactionId(heap_getattr(tuple,
                                MlogAttributeXid,
                                RelationGetDescr(mlog),
                                &isActionNULL));

        int64 seqno = DatumGetInt64(num);

        ereport(DEBUG5, (errcode(ERRCODE_SUCCESSFUL_COMPLETION),
                errmsg("Now seqno in Mlog is %ld.", seqno)));

        act = DatumGetChar(action);

        ItemPointer c_ptr = (ItemPointer)DatumGetPointer(ctid);

        Assert(!isCtidNull);

        if (isNull ||
            timestamp_cmp_internal(DatumGetTimestamp(oldTime),
                                   DatumGetTimestamp(refreshTime)) < 0) {
            HeapTuple reltuple;

            if (act == 'D') {
                /*
                 * Here we should CommandCounterIncrement when handle 'D' action in mlog,
                 * because those tuples which insert early should be visible here.
                 */
                CommandCounterIncrement();

                ExecutorMatMapDelete(mapid, relid, c_ptr, mvid, xid, qualsExist);
            } else if (mlog_form_tuple(mlog, tuple, rel, &reltuple)) {
                HeapTuple copyTuple = reltuple;
                copyTuple->t_self = *c_ptr;

                /*
                 * Need to run a recheck subquery.  Initialize or reinitialize EPQ state.
                 */
                EvalPlanQualBegin(epqstate, estate);

                /*
                 * Free old test tuple, if any, and store new tuple where relation's scan
                 * node will see it
                 */
                foreach (lc, rest) {
                    Index rti = (Index)lfirst_int(lc);

                    /* set non-epqs tuple to be NULL, should not execute */
                    EvalPlanQualSetTuple(epqstate, rti, NULL);
                }

                /* set tuple and prepare for run epq */
                EvalPlanQualSetTuple(epqstate, index, copyTuple);
                ExecutorMatEpqs(epqstate, copyTuple, relid, mapid, xid, matview, ActionRefreshInc);
                EvalPlanQualSetTuple(epqstate, index, NULL);
                EvalPlanQualEnd(epqstate);
            }

            if (isNull) {
                /* if time in mlog-table is NULL, then update it as current time. */
                update_mlog_time(mlog, tuple, curtime, indstate);
            }
        }
    }

    index_endscan(indexScan);
    CatalogCloseIndexes(indstate);
    index_close(mlogidx, NoLock);
    list_free_ext(indexoidlist);
    heap_close(rel, NoLock);
    heap_close(mlog, NoLock);

    pfree_ext(epqstate);
    list_free_ext(rest);
    return;
}

/*
 * Execute refresh matview incremental based on epq.
 */
static void ExecutorRefreshMatInc(QueryDesc* queryDesc, Query *query,
                                    Datum curtime, Relation matview, Oid mapid)
{
    Oid relid = InvalidOid;
    Oid mlogid = InvalidOid;
    Index index = 0;
    List *relids= NIL;
    ListCell *lc = NULL;

    if (queryDesc->plannedstmt->num_streams > 0) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Refresh Matview cannot support stream plan. ")));
    }

    /* find all based rels. */
    relids = pull_up_rels_recursive((Node *)query);

    bool qualsExist = CheckMatviewQuals(query);

    foreach (lc, relids) {
        relid = (Oid)lfirst_oid(lc);

        mlogid = find_mlog_table(relid);

        index = get_index_ref(queryDesc, relid);

        /*
         * FOR EXAMPLE:
         *   CREATE INC MATVIEW MV AS SELECT * FROM T1 WHERE 0 = 1 UNION
         *   ALL SELECT * FROM T2;
         */
        if (index == 0) {
            continue;
        }

        ExecutorMatRelInc(queryDesc, index, mlogid, relid, curtime, matview, mapid, qualsExist);
    }

    return;
}

void ExecRefreshMatViewInc(RefreshMatViewStmt *stmt, const char *queryString,
                 ParamListInfo params, char *completionTag)
{
    Oid matviewOid;
    Relation matviewRel;
    DestReceiver *dest = NULL;
    Query *query = NULL;
    PlannedStmt* plan = NULL;
    QueryDesc* queryDesc = NULL;
    bool isTimeNULL = false;
    Datum curtime;

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    /*
     * Get a lock until end of transaction.
     */
    matviewOid = RangeVarGetRelidExtended(stmt->relation,
                                           ExclusiveLock,
                                           false, false, false, false,
                                           RangeVarCallbackOwnsTable, NULL);

    Oid mapid = DatumGetObjectId(get_matview_mapid(matviewOid));
    Datum oldTime = get_matview_refreshtime(matviewOid, &isTimeNULL);
    if (timestamp_cmp_internal(DatumGetTimestamp(curtime),
            DatumGetTimestamp(oldTime)) <= 0) {
        return;
    }

    matviewRel = heap_open(matviewOid, ExclusiveLock);

    /* Make sure it is a materialized view. */
    if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW) {
       ereport(ERROR,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("\"%s\" is not a materialized view",
                       RelationGetRelationName(matviewRel))));
    }
    if (!is_incremental_matview(matviewOid)) {
       ereport(ERROR,
           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("\"%s\" is not an incremental materialized view",
                   RelationGetRelationName(matviewRel))));
    }

    if (IsSystemRelation(matviewRel)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("Not support materialized view \"%s\" is a system catalog relation",
                    RelationGetRelationName(matviewRel))));
    }
    Assert(!matviewRel->rd_rel->relhasoids);

    query = get_matview_query(matviewRel);
    Assert(IsA(query, Query));

    /* Copy it for ExecutorRefreshMatInc */
    Query *rquery = (Query *)copyObject(query);

    plan = GetPlanStmt(rquery, params);

    if (plan->num_streams > 0) {
       ereport(ERROR,
           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Refresh plan contains streams")));
    }

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    queryDesc = CreateQueryDesc(plan, queryString, GetActiveSnapshot(), InvalidSnapshot, dest, params, 0);

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, 0);

    ExecutorRefreshMatInc(queryDesc, query, curtime, matviewRel, mapid);

    /* done refresh then update matview of gs_matview. */
    update_matview_tuple(matviewOid, true, curtime);

    vacuum_mlog_for_matview(matviewOid);

    /* and clean up */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);

    heap_close(matviewRel, NoLock);

    return;
}

/*
 * ExecRefreshMatViewEpq -- execute a REFRESH MATERIALIZED VIEW command.
 * The Matview must be INCREMENTAL.
 */
void ExecRefreshIncMatViewAll(RefreshMatViewStmt *stmt, const char *queryString,
                            ParamListInfo params, char *completionTag)
{
    Oid mapid;
    Oid matviewOid;
    Relation matviewRel;
    DestReceiver *dest = NULL;
    Query *query = NULL;
    PlannedStmt* plan = NULL;
    QueryDesc* queryDesc = NULL;
    Datum curtime;

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    /*
     * Get a lock until end of transaction.
     */
    matviewOid = RangeVarGetRelidExtended(stmt->relation,
                                           AccessExclusiveLock,
                                           false, false, false, false,
                                           RangeVarCallbackOwnsTable, NULL);
    mapid = DatumGetObjectId(get_matview_mapid(matviewOid));
    matviewRel = heap_open(matviewOid, AccessExclusiveLock);

    /* Make sure it is a materialized view. */
    if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW) {
       ereport(ERROR,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("\"%s\" is not a materialized view",
                       RelationGetRelationName(matviewRel))));
    }
    if (!is_incremental_matview(matviewOid)) {
       ereport(ERROR,
           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("\"%s\" is not an incremental materialized view",
                   RelationGetRelationName(matviewRel))));
    }


    Assert(!IsSystemRelation(matviewRel));
    Assert(!matviewRel->rd_rel->relhasoids);

    query = get_matview_query(matviewRel);
    Assert(IsA(query, Query));

    /* Copy it for ExecutorRefreshMatInc */
    Query *rquery = (Query *)copyObject(query);

    plan = GetPlanStmt(rquery, params);

    if (plan->num_streams > 0) {
       ereport(ERROR,
           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Refresh plan contains streams")));
    }

    /* first delete matview all. */
    TableScanDesc scan;
    HeapTuple tuple;
    scan = tableam_scan_begin(matviewRel, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        simple_heap_delete(matviewRel, &tuple->t_self);
    }
    tableam_scan_end(scan);

    /* second clear matviewmap. */
    clearup_matviewmap_tuple(mapid);

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    queryDesc = CreateQueryDesc(plan, queryString, GetActiveSnapshot(), InvalidSnapshot, dest, params, 0);

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, 0);

    ExecCreateMatInc(queryDesc, query, matviewRel, mapid, curtime);

    /* done refresh then update matview of gs_matview. */
    update_matview_tuple(matviewOid, true, curtime);
    vacuum_mlog_for_matview(matviewOid);

    /* and clean up */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);
    FreeQueryDesc(queryDesc);

    heap_close(matviewRel, NoLock);

    return;
}

void ExecRefreshCtasMatViewAll(RefreshMatViewStmt *stmt, const char *queryString,
                 ParamListInfo params, char *completionTag)
{
    Oid matviewOid;
    Relation matviewRel;
    RewriteRule *rule = NULL;
    List *actions = NIL;
    Query *dataQuery = NULL;
    Oid tableSpace;
    Oid OIDNewHeap;
    DestReceiver *dest = NULL;
    Datum curtime;

    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    /*
     * Get a lock until end of transaction.
     */
    matviewOid = RangeVarGetRelidExtended(stmt->relation,
                                          AccessExclusiveLock, false, false, false, false,
                                          RangeVarCallbackOwnsTable, NULL);
    matviewRel = heap_open(matviewOid, NoLock);

    /* Make sure it is a materialized view. */
    if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("\"%s\" is not a materialized view",
                    RelationGetRelationName(matviewRel))));
    }

    update_matview_tuple(matviewOid, true, curtime);

    /*
     * We're not using materialized views in the system catalogs.
     */
    Assert(!IsSystemRelation(matviewRel));

    Assert(!matviewRel->rd_rel->relhasoids);

    /*
     * Check that everything is correct for a refresh. Problems at this point
     * are internal errors, so elog is sufficient.
     */
    if (matviewRel->rd_rel->relhasrules == false || matviewRel->rd_rules->numLocks < 1) {
        elog(ERROR,
            "materialized view \"%s\" is missing rewrite information",
            RelationGetRelationName(matviewRel));
    }

    if (matviewRel->rd_rules->numLocks > 1) {
        elog(ERROR,
            "materialized view \"%s\" has too many rules",
            RelationGetRelationName(matviewRel));
    }

    rule = matviewRel->rd_rules->rules[0];
    if (rule->event != CMD_SELECT || !(rule->isInstead)) {
        elog(ERROR,
            "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
            RelationGetRelationName(matviewRel));
    }

    actions = rule->actions;
    if (list_length(actions) != 1) {
        elog(ERROR,
            "the rule for materialized view \"%s\" is not a single action",
            RelationGetRelationName(matviewRel));
    }

    /*
     * The stored query was rewritten at the time of the MV definition, but
     * has not been scribbled on by the planner.
     */
    dataQuery = (Query *) linitial(actions);
    Assert(IsA(dataQuery, Query));

    /*
     * Check for active uses of the relation in the current transaction, such
     * as open scans.
     *
     * NB: We count on this to protect us against problems with refreshing the
     * data using HEAP_INSERT_FROZEN.
     */
    CheckTableNotInUse(matviewRel, "REFRESH MATERIALIZED VIEW");

    tableSpace = matviewRel->rd_rel->reltablespace;

    heap_close(matviewRel, NoLock);

    /* Create the transient table that will receive the regenerated data. */
    OIDNewHeap = make_new_heap(matviewOid, tableSpace);
    dest = CreateTransientRelDestReceiver(OIDNewHeap);

    if (!stmt->skipData) {
#ifdef ENABLE_MULTIPLE_NODES
        /* populate data to the transient table only on DNs */
        if (IS_PGXC_DATANODE)
#endif
            refresh_matview_datafill(dest, dataQuery, queryString);
    }

    /*
     * Swap the physical files of the target and transient tables, then
     * rebuild the target's indexes and throw away the transient table.
     */
    finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, u_sess->utils_cxt.RecentXmin, GetOldestMultiXactId());

    RelationCacheInvalidateEntry(matviewOid);
}

bool isIncMatView(RangeVar *rv)
{
    Oid matviewOid = RangeVarGetRelidExtended(rv,
                                           NoLock,
                                           false, false, false, false,
                                           RangeVarCallbackOwnsTable, NULL);
    Relation matviewRel = heap_open(matviewOid, AccessShareLock);

    /* Make sure it is a materialized view. */
    if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW) {
        heap_close(matviewRel, NoLock);
        ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("\"%s\" is not a materialized view",
                         RelationGetRelationName(matviewRel))));
    }

    heap_close(matviewRel, NoLock);

    return is_incremental_matview(matviewOid);
}

/*
 * ExecRefreshMatView -- execute a REFRESH MATERIALIZED VIEW command
 *
 * This refreshes the materialized view by creating a new table and swapping
 * the relfilenodes of the new table and the old materialized view, so the OID
 * of the original materialized view is preserved. Thus we do not lose GRANT
 * nor references to this materialized view.
 *
 * If WITH NO DATA was specified, this is effectively like a TRUNCATE;
 * otherwise it is like a TRUNCATE followed by an INSERT using the SELECT
 * statement associated with the materialized view.  The statement node's
 * skipData field is used to indicate that the clause was used.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new heap, it's better to create the indexes afterwards than to fill them
 * incrementally while we load.
 *
 * The scannable state is changed based on whether the contents reflect the
 * result set of the materialized view's query.
 */
void ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
                 ParamListInfo params, char *completionTag)
{
    if (isIncMatView(stmt->relation)) {
        return ExecRefreshIncMatViewAll(stmt, queryString, params, completionTag);
    } else {
        return ExecRefreshCtasMatViewAll(stmt, queryString, params, completionTag);
    }
}

static void ExecCreateMatInc(QueryDesc*queryDesc, Query *query, Relation matview,
                                    Oid mapid, Datum curtime)
{
    Relation rel;
    ListCell *lc = NULL;
    List *relids = NIL;
    TableScanDesc scan;
    HeapTuple tuple;
    Oid relid;
    Oid mlogid;
    Index index;

    /* find all based rels. */
    relids = pull_up_rels_recursive((Node *)query);

    foreach (lc, relids) {
        relid = (Oid)lfirst_oid(lc);
        rel = heap_open(relid, ExclusiveLock);
        mlogid = find_mlog_table(relid);
        EState *estate = queryDesc->estate;
        EPQState *epqstate = NULL;
        ListCell *idx = NULL;

        /* update tuples in mlog which refreshtime if NULL based on current */
        update_mlog_time_all(mlogid, curtime);

        index = get_index_ref(queryDesc, relid);

        /*
         * FOR EXAMPLE:
         *   CREATE INC MATVIEW MV AS SELECT * FROM T1 WHERE 0 = 1 UNION
         *   ALL SELECT * FROM T2;
         */
        if (index == 0) {
            continue;
        }

        /* pull-up rest indexes of rtables to be NULL. */
        List *rest = get_rest_index_ref(queryDesc, relid);

        epqstate = (EPQState *)palloc0(sizeof(EPQState));
        EvalPlanQualInit(epqstate, estate, queryDesc->plannedstmt->planTree, NIL, 0);
        epqstate->planstate = queryDesc->planstate;

        scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);

        while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            HeapTuple tmpTuple = NULL;
            /* here we want handle every tuple. */
            if (rel->rd_tam_type == TAM_USTORE) {
                tmpTuple = UHeapToHeap(rel->rd_att, (UHeapTuple)tuple);
                tmpTuple->t_xid_base = ((UHeapTuple)tuple)->t_xid_base;
                tmpTuple->t_data->t_choice.t_heap.t_xmin = ((UHeapTuple)tuple)->disk_tuple->xid;
                tuple = tmpTuple;
            }
            HeapTuple copyTuple = heap_copytuple(tuple);

            /*
             * Need to run a recheck subquery.  Initialize or reinitialize EPQ state.
             */
            EvalPlanQualBegin(epqstate, estate);

            foreach (idx, rest) {
                Index rti = (Index)lfirst_int(idx);
                /* set non-epqs tuple to be NULL, should not execute */
                EvalPlanQualSetTuple(epqstate, rti, NULL);
            }

            EvalPlanQualSetTuple(epqstate, index, copyTuple);
            ExecutorMatEpqs(epqstate, copyTuple, relid, mapid, HeapTupleGetRawXmin(copyTuple), matview, ActionCreateMat);
            EvalPlanQualSetTuple(epqstate, index, NULL);
            if (tmpTuple != NULL) {
                tableam_tops_free_tuple(tmpTuple);
                tmpTuple = NULL;
            }
        }

        tableam_scan_end(scan);
        heap_close(rel, NoLock);

        EvalPlanQualEnd(epqstate);
        pfree_ext(epqstate);
        list_free_ext(rest);
    }

    list_free_ext(relids);
    return;
}

void ExecCreateMatViewInc(CreateTableAsStmt* stmt, const char* queryString, ParamListInfo params)
{
    Datum curtime;
    Relation matview;
    Oid matviewid;
    Oid mapid;
    Query* query = (Query*)stmt->query;
    IntoClause* into = stmt->into;
    QueryDesc* queryDesc = NULL;
    PlannedStmt* plan = NULL;
    DestReceiver* dest = NULL;

    /* Get current timestamp */
    curtime = DirectFunctionCall1(timestamptz_timestamp, GetCurrentTimestamp());

    /*
     * Create the tuple receiver object and insert info it will need
     */
    dest = CreateIntoRelDestReceiver(into);

    query = SetupForCreateTableAs(query, into, queryString, params, dest);

    List *relids = pull_up_rels_recursive((Node *)query);

    ListCell *lc = NULL;
    foreach (lc, relids) {
        Oid relid = (Oid)lfirst_oid(lc);
        Relation rel = heap_open(relid, ExclusiveLock);
        heap_close(rel, NoLock);
    }
    /* Copy it for ExecutorRefreshMatInc */
    Query *rquery = (Query *)copyObject(query);

    plan = GetPlanStmt(rquery, params);

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    queryDesc = CreateQueryDesc(plan, queryString, GetActiveSnapshot(), InvalidSnapshot, dest, params, 0);

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, 0);

    /* Build matview first. */
    (*dest->rStartup)(dest, queryDesc->operation, queryDesc->tupDesc);

    DR_intorel* myState = (DR_intorel*)dest;
    matview = myState->rel;
    matviewid = RelationGetRelid(matview);
    mapid = DatumGetObjectId(get_matview_mapid(matviewid));

    /* update matview metadata of gs_matview. */
    update_matview_tuple(matviewid, true, curtime);

    ExecCreateMatInc(queryDesc, query, matview, mapid, curtime);

    (*dest->rShutdown)(dest);

    /* and clean up */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);

    /* find all based rels */
    relids = pull_up_rels_recursive((Node *)query);

    foreach (lc, relids) {
        Oid base_oid = (Oid)lfirst_oid(lc);
        CacheInvalidateRelcacheByRelid(base_oid);
    }

    CommandCounterIncrement();
}

/*
 * refresh_matview_datafill
 */
static void refresh_matview_datafill(DestReceiver *dest, Query *query, const char *queryString)
{
    List *rewritten;
    PlannedStmt *plan = NULL;
    QueryDesc *queryDesc = NULL;
    List *rtable = NIL;
    RangeTblEntry *initial_rte = NULL;
    RangeTblEntry *second_rte = NULL;

    rewritten = QueryRewrite((Query *) copyObject(query));

    /* SELECT should never rewrite to more or less than one SELECT query */
    if (list_length(rewritten) != 1) {
        elog(ERROR, "unexpected rewrite result for REFRESH MATERIALIZED VIEW");
    }
    query = (Query *) linitial(rewritten);

    /* Check for user-requested abort. */
    CHECK_FOR_INTERRUPTS();

    /*
     * Kludge here to allow refresh of a materialized view which is invalid
     * (that is, it was created or refreshed WITH NO DATA. We flag the first
     * two RangeTblEntry list elements, which were added to the front of the
     * rewritten Query to keep the rules system happy, with the isResultRel
     * flag to indicate that it is OK if they are flagged as invalid. See
     * UpdateRangeTableOfViewParse() for details.
     *
     * NOTE: The rewrite has switched the frist two RTEs, but they are still
     * in the first two positions. If that behavior changes, the asserts here
     * will fail.
     */
    rtable = query->rtable;
    initial_rte = ((RangeTblEntry *) linitial(rtable));
    Assert(strcmp(initial_rte->alias->aliasname, "new"));
    initial_rte->isResultRel = true;
    second_rte = ((RangeTblEntry *) lsecond(rtable));
    Assert(strcmp(second_rte->alias->aliasname, "old"));
    second_rte->isResultRel = true;

    plan = GetPlanStmt(query, NULL);

    /*
     * Use a snapshot with an updated command ID to ensure this query sees
     * results of any previously executed queries.  (This could only matter if
     * the planner executed an allegedly-stable function that changed the
     * database contents, but let's do it anyway to be safe.)
    */
    PushCopiedSnapshot(GetActiveSnapshot());
    UpdateActiveSnapshotCommandId();

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    queryDesc = CreateQueryDesc(plan, queryString,
                                GetActiveSnapshot(), InvalidSnapshot,
                                dest, NULL, 0);

    /* call ExecutorStart to prepare the plan for execution */
    ExecutorStart(queryDesc, EXEC_FLAG_WITHOUT_OIDS);

    /* run the plan */
    ExecutorRun(queryDesc, ForwardScanDirection, 0L);

    /* and clean up */
    ExecutorFinish(queryDesc);
    ExecutorEnd(queryDesc);

    FreeQueryDesc(queryDesc);
    list_free_ext(rewritten);

    PopActiveSnapshot();
}

DestReceiver *CreateTransientRelDestReceiver(Oid transientoid)
{
   DR_transientrel *self = (DR_transientrel *) palloc0(sizeof(DR_transientrel));

   self->pub.receiveSlot = transientrel_receive;
   self->pub.rStartup = transientrel_startup;
   self->pub.rShutdown = transientrel_shutdown;
   self->pub.rDestroy = transientrel_destroy;
   self->pub.mydest = DestTransientRel;
   self->transientoid = transientoid;

   return (DestReceiver *) self;
}

/*
 * transientrel_startup --- executor startup
 */
static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
   DR_transientrel *myState = (DR_transientrel *) self;
   Relation transientrel;

   transientrel = heap_open(myState->transientoid, NoLock);

   /*
    * Fill private fields of myState for use by later routines
    */
   myState->transientrel = transientrel;
   myState->output_cid = GetCurrentCommandId(true);

   /*
    * We can skip WAL-logging the insertions, unless PITR or streaming
    * replication is in use. We can skip the FSM in any case.
    */
   myState->hi_options = HEAP_INSERT_SKIP_FSM | HEAP_INSERT_FROZEN;
   if (!XLogIsNeeded()) {
       myState->hi_options |= HEAP_INSERT_SKIP_WAL;
   }
   myState->bistate = GetBulkInsertState();

   SetRelationIsScannable(transientrel);

   /* Not using WAL requires smgr_targblock be initially invalid */
   Assert(RelationGetTargetBlock(transientrel) == InvalidBlockNumber);
}

/*
 * transientrel_receive --- receive one tuple
 */
static void transientrel_receive(TupleTableSlot *slot, DestReceiver *self)
{
   DR_transientrel *myState = (DR_transientrel *) self;
   HeapTuple   tuple;

   /*
    * get the heap tuple out of the tuple table slot, making sure we have a
    * writable copy
    */
   tuple = ExecMaterializeSlot(slot);

   tableam_tuple_insert(myState->transientrel,
               tuple,
               myState->output_cid,
               myState->hi_options,
               myState->bistate);

   /* We know this is a newly created relation, so there are no indexes */
}

/*
 * transientrel_shutdown --- executor end
 */
static void transientrel_shutdown(DestReceiver *self)
{
   DR_transientrel *myState = (DR_transientrel *) self;

   FreeBulkInsertState(myState->bistate);

   /* If we skipped using WAL, must heap_sync before commit */
   if ((unsigned int)myState->hi_options & HEAP_INSERT_SKIP_WAL) {
       heap_sync(myState->transientrel);
   }

   /* close transientrel, but keep lock until commit */
   heap_close(myState->transientrel, NoLock);
   myState->transientrel = NULL;
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
static void transientrel_destroy(DestReceiver *self)
{
   pfree(self);
}

/*
 * pull-up RTE_RELATION rels from query.
 */
List *pull_up_rels_recursive(Node *node)
{
    List *relids = NIL;

	if (node == NULL)
	{
		return relids;
	}

    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry *)node;

        /* also need recursive query. */
        if (rte->rtekind == RTE_SUBQUERY) {
            relids = pull_up_rels_recursive((Node *)rte->subquery);
        }

        /* add RTE_RELATION 'r' to relids. */
        if (rte->rtekind == RTE_RELATION && rte->relkind == 'r') {
            relids = lappend_oid(relids, rte->relid);
        }
    }

    if (IsA(node, Query)) {
        Query *query = (Query *)node;
        ListCell *lc = NULL;

        foreach (lc, query->rtable) {
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

            List *round_relids = pull_up_rels_recursive((Node *)rte);
            relids = list_union_oid(relids, round_relids);
        }
    }

    return relids;
}

/*
 * create mlog-table based on rel-table.
 */
static Oid create_mlog_table(Oid relid)
{
    int i;
    int j;
    Oid mlogid = 0;
    errno_t rc = EOK;
    TupleDesc tupdesc = NULL;
    Relation mlog_rel;
    Datum reloptions = (Datum)0;
    char mlogname[NAMEDATALEN];
    char mlog_idxname[NAMEDATALEN];
    Oid tablespaceid = InvalidOid;
    IndexInfo* indexInfo = NULL;
    Oid collationObjectId[1];
    Oid classObjectId[1];
    int16 coloptions[1];

    /* RowExclusiveLock is OK */
    Relation rel = heap_open(relid, RowExclusiveLock);

    rc = snprintf_s(mlogname, sizeof(mlogname), sizeof(mlogname) - 1, "mlog_%u", relid);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(mlog_idxname, sizeof(mlog_idxname), sizeof(mlog_idxname) - 1, "mlog_%u_index", relid);
    securec_check_ss(rc, "\0", "\0");

    tablespaceid = rel->rd_rel->reltablespace;

    TupleDesc relDesc = rel->rd_att;
    Form_pg_attribute* relAtts = relDesc->attrs;

    int relAttnumAll = relDesc->natts;
    int relAttnum = relDesc->natts;
    for (i = 0; i < relAttnumAll; i++) {
        if (relAtts[i]->attisdropped) {
            relAttnum--;
        }
    }

    tupdesc = CreateTemplateTupleDesc(MlogAttributeNum + relAttnum, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)MlogAttributeAction, "action", CHAROID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MlogAttributeTime, "refresh_time", TIMESTAMPOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MlogAttributeCtid, "tup_ctid", TIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MlogAttributeXid, "xid", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MlogAttributeSeqno, "seqno", INT8OID, -1, 0);

    tupdesc->attrs[MlogAttributeAction - 1]->attstorage = 'p';
    tupdesc->attrs[MlogAttributeTime - 1]->attstorage = 'p';
    tupdesc->attrs[MlogAttributeCtid - 1]->attstorage = 'p';
    tupdesc->attrs[MlogAttributeXid - 1]->attstorage = 'p';
    tupdesc->attrs[MlogAttributeSeqno - 1]->attstorage = 'p';

    for (i = 1, j = 1; i <= relAttnumAll; i++) {
        if (relAtts[i - 1]->attisdropped) {
            continue;
        }
        TupleDescInitEntry(tupdesc, (AttrNumber)MlogAttributeNum + j,
                    NameStr(relAtts[i - 1]->attname), relAtts[i - 1]->atttypid,
                    relAtts[i - 1]->atttypmod, relAtts[i - 1]->attndims);
        tupdesc->attrs[MlogAttributeNum + j - 1]->attstorage = relAtts[i - 1]->attstorage;
        j++;
    }

    /* add internal_mask_enable */
    reloptions = AddInternalOption(reloptions, INTERNAL_MASK_DDELETE |
        INTERNAL_MASK_DINSERT | INTERNAL_MASK_DUPDATE);
    reloptions = AddOrientationOption(reloptions, false);

    mlogid = heap_create_with_catalog(mlogname,
        rel->rd_rel->relnamespace,
        rel->rd_rel->reltablespace,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        rel->rd_rel->relowner,
        tupdesc,
        NIL,
        RELKIND_RELATION,
        rel->rd_rel->relpersistence,
        rel->rd_rel->relisshared,
        false,
        true,
        0,
        ONCOMMIT_NOOP,
        reloptions,
        false,
        true,
        NULL,
        REL_CMPRS_NOT_SUPPORT,
        NULL,
        true);

    /* make the mlog relation visible, else heap_open will fail */
    CommandCounterIncrement();

    /* try to create index on seqno column of mlog-table */
    mlog_rel = heap_open(mlogid, ShareLock);

    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = 1;
    indexInfo->ii_NumIndexKeyAttrs = 1;
    indexInfo->ii_KeyAttrNumbers[0] = MlogAttributeSeqno;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = NIL;
    indexInfo->ii_PredicateState = NIL;
    indexInfo->ii_ExclusionOps = NULL;
    indexInfo->ii_ExclusionProcs = NULL;
    indexInfo->ii_ExclusionStrats = NULL;
    indexInfo->ii_Unique = true;
    indexInfo->ii_ReadyForInserts = true;
    indexInfo->ii_Concurrent = false;
    indexInfo->ii_BrokenHotChain = false;
    indexInfo->ii_PgClassAttrId = 0;
    indexInfo->ii_ParallelWorkers = 0;

    collationObjectId[0] = InvalidOid;
    classObjectId[0] = INT8_BTREE_OPS_OID;
    coloptions[0] = 0;

    IndexCreateExtraArgs extra;
    SetIndexCreateExtraArgs(&extra, InvalidOid, false, false);

    index_create(mlog_rel,
        mlog_idxname,
        InvalidOid,
        InvalidOid,
        indexInfo,
        list_make1((void*)"seqno"),
        BTREE_AM_OID,
        rel->rd_rel->reltablespace,
        collationObjectId,
        classObjectId,
        coloptions,
        (Datum)0,
        true,
        false,
        false,
        false,
        true,
        false,
        false,
        &extra,
        false);

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        DistributeBy *distributeby = NULL;
        PGXCSubCluster* subcluster = NULL;
        distributeby = makeNode(DistributeBy);
        distributeby->disttype = DISTTYPE_HASH;
        List *colnames = NIL;

        for (int i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute attr = tupdesc->attrs[i];
            if(IsTypeDistributable(attr->atttypid)) {
                Value *colValue = makeString(pstrdup(attr->attname.data));
                colnames = lappend(colnames, colValue);
                break;
            }
        }
        distributeby->colname = colnames;

        /* handle node group infos */
        Oid group_id = ng_get_baserel_groupoid(relid, RELKIND_RELATION);
        char *group_name = ng_get_group_group_name(group_id);
        subcluster = makeNode(PGXCSubCluster);
        subcluster->clustertype = SUBCLUSTER_GROUP;
        subcluster->members = list_make1(makeString(group_name));

        AddRelationDistribution(mlogname, mlogid, distributeby, subcluster, NIL, tupdesc, false);
    }
#endif

    heap_close(mlog_rel, NoLock);
    heap_close(rel, NoLock);

    CacheInvalidateRelcacheByRelid(relid);

    AlterTableCreateToastTable(mlogid, reloptions);

    /*
     * Make changes visible
     */
    CommandCounterIncrement();

    return mlogid;
}

static Oid find_mlog_table(Oid relid)
{
    Oid mlogid;
    HeapTuple tup;
    TableScanDesc scan;
    Relation matview_dep;
    Form_gs_matview_dependency matviewdepForm;

    matview_dep = heap_open(MatviewDependencyId, AccessShareLock);
    scan = tableam_scan_begin(matview_dep, SnapshotNow, 0, NULL);
    tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);

    while(tup != NULL) {
        matviewdepForm = (Form_gs_matview_dependency)GETSTRUCT(tup);

        if (matviewdepForm->relid == relid) {
            mlogid = matviewdepForm->mlogid;
            break;
        }
        tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection);
    }

    if (tup != NULL && HeapTupleIsValid(tup)) {
        tableam_scan_end(scan);
        heap_close(matview_dep, NoLock);
        return mlogid;
    }

    tableam_scan_end(scan);
    heap_close(matview_dep, NoLock);

    /* if not found, create mlog-table. */
    mlogid = create_mlog_table(relid);

    return mlogid;
}

/*
 * build gs_matview_dependency relationship table.
 * 1 pull-up rels from query.
 * 2 insert relationship tuples into gs_matview_dependency.
 */
void build_matview_dependency(Oid matviewOid, Relation matviewRelation)
{
    /* get action of matview first. */
    Query *query = get_matview_query(matviewRelation);

    List *relids = pull_up_rels_recursive((Node *)query);

    ListCell *lc = NULL;

    /*
     * build pg_materview_dependency
     * 1 find mlog-table based on relid, if not exists then create mlog-table.
     * 2 insert dependencys of these tables into pg_materview_dependency.
     */
    foreach (lc, relids) {
        Oid relid = (Oid)lfirst_oid(lc);

        Oid mlogid = find_mlog_table(relid);

        insert_matviewdep_tuple(matviewOid, relid, mlogid);
    }

    return;
}

Oid create_matview_map(Oid matviewoid)
{
    Oid mapId;
    errno_t rc = EOK;
    TupleDesc tupdesc;
    Datum reloptions = (Datum)0;
    char matviewmap[NAMEDATALEN];
    char matviewMapIndex[NAMEDATALEN];
    Relation mapRel;
    IndexInfo* indexInfo = NULL;

    Relation rel = heap_open(matviewoid, ShareLock);

    rc = snprintf_s(matviewmap, sizeof(matviewmap), sizeof(matviewmap) - 1, "matviewmap_%u", matviewoid);
    securec_check_ss(rc, "\0", "\0");
    rc = snprintf_s(matviewMapIndex, sizeof(matviewMapIndex), sizeof(matviewMapIndex) - 1,
                    "matviewmap_%u_index", matviewoid);
    securec_check_ss(rc, "\0", "\0");

    tupdesc = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)MatMapAttributeMatid, "matid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MatMapAttributeMatctid, "mat_ctid", TIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MatMapAttributeRelid, "relid", OIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MatMapAttributeRelctid, "rel_ctid", TIDOID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)MatMapAttributeRelxid, "rel_xid", XIDOID, -1, 0);

    tupdesc->attrs[0]->attstorage = 'p';
    tupdesc->attrs[1]->attstorage = 'p';
    tupdesc->attrs[2]->attstorage = 'p';
    tupdesc->attrs[3]->attstorage = 'p';
    tupdesc->attrs[4]->attstorage = 'p';

    /* add internal_mask_enable */
    reloptions = AddInternalOption(reloptions, INTERNAL_MASK_DDELETE |
        INTERNAL_MASK_DINSERT | INTERNAL_MASK_DUPDATE);
    reloptions = AddOrientationOption(reloptions, false);

    mapId = heap_create_with_catalog(matviewmap,
        rel->rd_rel->relnamespace,
        rel->rd_rel->reltablespace,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        rel->rd_rel->relowner,
        tupdesc,
        NIL,
        RELKIND_RELATION,
        rel->rd_rel->relpersistence,
        rel->rd_rel->relisshared,
        false,
        true,
        0,
        ONCOMMIT_NOOP,
        reloptions,
        false,
        true,
        NULL,
        REL_CMPRS_NOT_SUPPORT,
        NULL,
        true);

    CommandCounterIncrement();

    mapRel = heap_open(mapId, ShareLock);

    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = 3;
    indexInfo->ii_NumIndexKeyAttrs = 3;
    indexInfo->ii_KeyAttrNumbers[0] = MatMapAttributeMatid;
    indexInfo->ii_KeyAttrNumbers[1] = MatMapAttributeRelid;
    indexInfo->ii_KeyAttrNumbers[2] = MatMapAttributeRelctid;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = NIL;
    indexInfo->ii_PredicateState = NIL;
    indexInfo->ii_ExclusionOps = NULL;
    indexInfo->ii_ExclusionProcs = NULL;
    indexInfo->ii_ExclusionStrats = NULL;
    indexInfo->ii_Unique = true;
    indexInfo->ii_ReadyForInserts = true;
    indexInfo->ii_Concurrent = false;
    indexInfo->ii_BrokenHotChain = false;
    indexInfo->ii_PgClassAttrId = 0;
    indexInfo->ii_ParallelWorkers = 0;

    Oid collationObjectId[3] = {InvalidOid, InvalidOid, InvalidOid};
    Oid classObjectId[3] = {OID_BTREE_OPS_OID, OID_BTREE_OPS_OID, GetDefaultOpClass(TIDOID, BTREE_AM_OID)};
    int16 colOptions[3] = {0, 0, 0};

    IndexCreateExtraArgs extra;
    SetIndexCreateExtraArgs(&extra, InvalidOid, false, false);

    (void) index_create(mapRel,
                        matviewMapIndex,
                        InvalidOid,
                        InvalidOid,
                        indexInfo,
                        list_make3((void*)"matid", (void*)"relid", (void*)"relctid"),
                        BTREE_AM_OID,
                        rel->rd_rel->reltablespace,
                        collationObjectId,
                        classObjectId,
                        colOptions,
                        (Datum)0,
                        false,
                        false,
                        false,
                        false,
                        true,
                        false,
                        false,
                        &extra,
                        false);

    heap_close(mapRel, ShareLock);

    CommandCounterIncrement();

#ifdef ENABLE_MULTIPLE_NODES
    if (IS_PGXC_COORDINATOR) {
        DistributeBy *distributeby = NULL;
        PGXCSubCluster* subcluster = NULL;
        distributeby = makeNode(DistributeBy);
        distributeby->disttype = DISTTYPE_ROUNDROBIN;
        distributeby->colname = NULL;

        /* handle node group infos */
        Oid group_id = ng_get_baserel_groupoid(matviewoid, RELKIND_RELATION);
        char *group_name = ng_get_group_group_name(group_id);
        subcluster = makeNode(PGXCSubCluster);
        subcluster->clustertype = SUBCLUSTER_GROUP;
        subcluster->members = list_make1(makeString(group_name));

        AddRelationDistribution(matviewmap, mapId, distributeby, subcluster, NIL, tupdesc, false);
    }
#endif

    heap_close(rel, ShareLock);

    CommandCounterIncrement();

    return mapId;
}

void insert_into_matview_map(Oid mapid, Oid matid, ItemPointer matctid,
                                    Oid relid, ItemPointer relctid, TransactionId xid)
{
    errno_t     rc;
    Relation mapRel;
    Relation mapIdx;
    HeapTuple tup;
    Oid indexOid;
    Datum values[MatMapAttributeNum];
    bool isNulls[MatMapAttributeNum];

    mapRel = heap_open(mapid, RowExclusiveLock);
    List *indexOidList = RelationGetIndexList(mapRel);

    if (RelationGetNumberOfAttributes(mapRel) != MatMapAttributeNum) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("recreate matview which matoid is %u because matmap column nums inconsistent.", matid)));
    }

    if (indexOidList == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can't find index on %s", RelationGetRelationName(mapRel))));
    }
    indexOid = (Oid)linitial_oid(indexOidList);

    mapIdx = index_open(indexOid, RowExclusiveLock);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc =  memset_s(isNulls, sizeof(isNulls), false, sizeof(isNulls));
    securec_check(rc, "\0", "\0");

    values[MatMapAttributeMatid - 1] = ObjectIdGetDatum(matid);
    values[MatMapAttributeMatctid - 1] = ItemPointerGetDatum(matctid);
    values[MatMapAttributeRelid - 1] = ObjectIdGetDatum(relid);
    values[MatMapAttributeRelctid - 1] = ItemPointerGetDatum(relctid);
    values[MatMapAttributeRelxid - 1] = TransactionIdGetDatum(xid);

    tup = heap_form_tuple(mapRel->rd_att, values, isNulls);

    /* 1. insert simple tuple into matmap. */
    (void)simple_heap_insert(mapRel, tup);

    /* prepare index info data. */
    Datum idxValues[3];
    bool idxIsNulls[3];
    idxValues[0] = ObjectIdGetDatum(matid);
    idxValues[1] = ObjectIdGetDatum(relid);
    idxValues[2] = ItemPointerGetDatum(relctid);
    idxIsNulls[0] = false;
    idxIsNulls[1] = false;
    idxIsNulls[2] = false;

    /* 2. insert index tuple into maptmap-index */
    (void) index_insert(mapIdx,
                        idxValues,
                        idxIsNulls,
                        &(tup->t_self),
                        mapRel,
                        UNIQUE_CHECK_YES);

    heap_freetuple_ext(tup);
    list_free_ext(indexOidList);

    index_close(mapIdx, NoLock);
    heap_close(mapRel, NoLock);
    return;
}

Oid find_matview_mlog_table(Oid relid)
{
    Oid mlogid = InvalidOid;
    HeapTuple tup;
    ScanKeyData scanKey;
    SysScanDesc scan;
    Relation rel;
    int rc;
    char mlogname[NAMEDATALEN];

    rc = snprintf_s(mlogname, sizeof(mlogname), sizeof(mlogname) - 1, "mlog_%u", relid);
    securec_check_ss(rc, "\0", "\0");

    ScanKeyInit(&scanKey, Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(mlogname));
    rel = heap_open(RelationRelationId, AccessShareLock);
    scan = systable_beginscan(rel, ClassNameNspIndexId, true, NULL, 1, &scanKey);
    tup = systable_getnext(scan);

    if (HeapTupleIsValid(tup)) {
        mlogid = HeapTupleGetOid(tup);
    }
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return mlogid;
}

void insert_into_mlog_table(Relation rel, Oid mlogid, HeapTuple tuple, ItemPointer tid, TransactionId xid, char action)
{
    int i;
    int j;
    errno_t rc;
    Relation mlog_table;
    Relation mlog_idx;
    Oid indexoid;
    HeapTuple htup;
    int64 seqno = 0;

    TupleDesc relDesc = rel->rd_att;
    int relAttnumAll = relDesc->natts;
    int relAttnum = relDesc->natts;
    for (i = 0; i < relAttnumAll; i++) {
        if (relDesc->attrs[i]->attisdropped) {
            relAttnum--;
        }
    }

    Datum values[MlogAttributeNum + relAttnum];
    bool isnulls[MlogAttributeNum + relAttnum];

    mlog_table = try_relation_open(mlogid, RowExclusiveLock);
    if (!RelationIsValid(mlog_table)) {
        return;
    }

    List *indexoidlist = RelationGetIndexList(mlog_table);
    if (indexoidlist == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can't find index on %s", RelationGetRelationName(mlog_table))));
    }
    indexoid = (Oid)linitial_oid(indexoidlist);

    mlog_idx = index_open(indexoid, RowExclusiveLock);

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc =  memset_s(isnulls, sizeof(isnulls), false, sizeof(isnulls));
    securec_check(rc, "\0", "\0");

    values[MlogAttributeAction - 1] = CharGetDatum(action);
    isnulls[MlogAttributeTime - 1] = true;
    values[MlogAttributeCtid - 1] = PointerGetDatum(tid);
    values[MlogAttributeXid - 1] = TransactionIdGetDatum(xid);

    seqno = MlogGetSeqNo();

    values[MlogAttributeSeqno - 1] = Int64GetDatum(seqno);

    /* insert tuple content to the mlog */
    if (action == 'I') {
        Datum rel_values[relAttnumAll];
        bool rel_isnulls[relAttnumAll];

        Assert(tuple != NULL);

        heap_deform_tuple(tuple, relDesc, rel_values, rel_isnulls);

        for (i = 0, j = 0; i < relAttnumAll; i++) {
            if (relDesc->attrs[i]->attisdropped) {
                ereport(DEBUG5,
                        (errmodule(MOD_OPT), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Skip dropped column %d on base table when insert into mlog table", i)));
                continue;
            }
            values[MlogAttributeNum + j] = rel_values[i];
            isnulls[MlogAttributeNum + j] = rel_isnulls[i];
            j++;
        }
    } else {
        for (i = 0; i < relAttnum; i++) {
            isnulls[MlogAttributeNum + i] = true;
        }
    }

    htup = heap_form_tuple(mlog_table->rd_att, values, isnulls);

    /* 1 insert simple tuple into mlog-table. */
    (void)simple_heap_insert(mlog_table, htup);

    /* prepare index info data. */
    Datum idxvalues[INDEX_MAX_KEYS];
    bool idxisnull[INDEX_MAX_KEYS];
    idxvalues[0] = Int64GetDatum(seqno);
    idxisnull[0] = false;

    /* 2 insert index tuple into mlog-table */
    index_insert(mlog_idx,
                idxvalues,
                idxisnull,
                &(htup->t_self),
                mlog_table,
                UNIQUE_CHECK_YES);

    heap_freetuple_ext(htup);
    list_free_ext(indexoidlist);
    index_close(mlog_idx, NoLock);
    heap_close(mlog_table, RowExclusiveLock);
    return;
}

static void update_mlog_time_all(Oid mlogid, Datum curtime)
{
    Relation mlog;
    Oid indexoid;
    Relation mlogidx;
    HeapTuple tuple;
    bool isNull = false;
    IndexScanDesc indexScan;

    mlog = heap_open(mlogid, RowExclusiveLock);
    List *indexoidlist = RelationGetIndexList(mlog);
    if (indexoidlist == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can't find index on %s", RelationGetRelationName(mlog))));
    }
    indexoid = (Oid)linitial_oid(indexoidlist);

    mlogidx = index_open(indexoid, RowExclusiveLock);
    CatalogIndexState indstate = CatalogOpenIndexes(mlog);

    indexScan = index_beginscan(mlog, mlogidx, SnapshotNow, 0, 0);
    index_rescan(indexScan, NULL, 0, NULL, 0);

    while ((tuple = (HeapTuple)index_getnext(indexScan, ForwardScanDirection)) != NULL) {
        Datum refreshTime = heap_getattr(tuple,
                                        MlogAttributeTime,
                                        RelationGetDescr(mlog),
                                        &isNull);

        /* update refreshtime */
        if (isNull || (void*)refreshTime == NULL) {
            update_mlog_time(mlog, tuple, curtime, indstate);
        }
    }

    list_free_ext(indexoidlist);
    index_endscan(indexScan);
    CatalogCloseIndexes(indstate);
    index_close(mlogidx, RowExclusiveLock);
    heap_close(mlog, NoLock);
    return;
}

static void update_mlog_time(Relation mlog, HeapTuple tuple, Datum curtime, CatalogIndexState indstate)
{
    errno_t rc;
    HeapTuple ntup = NULL;
    Datum values[mlog->rd_att->natts];
    bool replaces[mlog->rd_att->natts];
    bool nulls[mlog->rd_att->natts];

    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "\0", "\0");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "\0", "\0");

    replaces[MlogAttributeTime - 1] = true;
    values[MlogAttributeTime - 1] = curtime;

    ntup = heap_modify_tuple(tuple, RelationGetDescr(mlog), values, nulls, replaces);

    simple_heap_update(mlog, &ntup->t_self, ntup);
    CatalogIndexInsert(indstate, ntup);
    heap_freetuple_ext(ntup);
    return;
}

void check_matview_op_supported(CreateTableAsStmt *ctas) {
    Oid groupid = 0;
    Query *rootQry = NULL;
    List *distkeyList = NULL;
    List *rangeTables = NULL;

    // assert ctas->query != NULL;
    rootQry = (Query*)ctas->query;
    if (rootQry->setOperations == NULL) {
        check_simple_query(rootQry, &distkeyList, &rangeTables, &groupid);
    } else {
        check_union_all(rootQry, &distkeyList, &rangeTables, &groupid);
    }

    ctas->groupid = groupid;
    list_free(distkeyList);
}

static void check_union_all(Query *query, List **distkeyList, List **rangeTables, Oid *groupid) {
    ListCell *lc = NULL;

    // assert query->setOperations != NULL;
    check_set_op_component(query->setOperations);
    foreach (lc, query->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
        check_simple_query(rte->subquery, distkeyList, rangeTables, groupid);
    }
}

static void check_set_op_component(Node *node) {
    if (IsA(node, SetOperationStmt)) {
        SetOperationStmt *setop = (SetOperationStmt *)node;
        if ((setop->op != SETOP_UNION) || !setop->all) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("Only UNION ALL is supported on incremental materialized views")));
        }
        check_set_op_component(setop->larg);
        check_set_op_component(setop->rarg);
    }
}

static void check_simple_query(Query *query, List **refDistkeyList,
                                       List **rangeTables, Oid *groupid) {
    RangeTblEntry *rte = NULL;

    if (query->cteList != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("with or start with clause")));
    }

    if (query->returningList != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("returning clause")));
    }
    if (query->groupClause != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("group clause")));
    }
    if (query->windowClause != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("window clause")));
    }
    if (query->distinctClause != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("distinct clause")));
    }
    if (query->sortClause != NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("sort clause")));
    }
    if ((query->limitOffset != 0) || (query->limitCount != 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("limit clause")));
    }
    if (query->commandType != CMD_SELECT) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Only SELECT is supported on incremental materialized views")));
    }

    if (query->hasSubLinks) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("complicated subquery is not supported, except UNION ALL")));
    }
    if (query->hasAggs) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("aggregates on incremental materialized view creation")));
    }
    if (list_length(query->rtable) > 1) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Feature not supported"),
                errdetail("Join is not supported on incremental materialized views")));
    }
    if (query->rtable == NULL) {
        // skip validation of distribute key if SELECT constants
        return;
    }

    rte = (RangeTblEntry *)linitial(query->rtable);

    check_table(rte, groupid);
    Assert(rte->relname != NULL);

    if (list_member_oid(*rangeTables, rte->relid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("relation \"%s\" is selected more than once", rte->relname)));
    }
    *rangeTables = lappend_oid(*rangeTables, rte->relid);

#ifdef ENABLE_MULTIPLE_NODES
    ListCell *lc = NULL;
    ListCell *ic = NULL;
    List *distkeyInfo = NULL;
    List *distkeyList = NULL;

    // check for target list
    distkeyInfo = get_distkey_info(rte->relid);
    Assert(distkeyInfo != NULL);

    // for each distkey of the table, find the corresponding column in the target list.
    foreach (ic, distkeyInfo) {
        DistKeyInfo *dkInfo = (DistKeyInfo *)lfirst(ic);

        foreach (lc, query->targetList) {
            TargetEntry *te = (TargetEntry *)lfirst(lc);
            if (IsA(te->expr, Var) && (te->resorigcol == dkInfo->attnum)) {
                distkeyList = lappend_int(distkeyList, te->resno);
                break;
            }
        }
    }

    if (list_length(distkeyInfo) != list_length(distkeyList)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Target list does not contain all the distribute keys of table \"%s\"", rte->relname)));
    }

    if (*refDistkeyList == NULL) {
        // this is the first subquery encountered
        *refDistkeyList = distkeyList;
    } else {
        // distkeyList and *refDistkeyList should contain the same elements,
        // which indicates that the projection on each subquery contain the same distribute key
        if (list_length(distkeyList) != list_length(*refDistkeyList)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("A query on \"%s\" does not contain the same distribute keys as others", rte->relname)));
        }
        forboth (lc, distkeyList, ic, *refDistkeyList) {
            AttrNumber distKey = (AttrNumber)lfirst_int(lc);
            AttrNumber refDistkey = (AttrNumber)lfirst_int(ic);
            if (distKey != refDistkey) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("A query on \"%s\" does not contain the same distribute keys as others", rte->relname)));
            }
        }
    }

    if (*refDistkeyList != distkeyList) {
        list_free(distkeyList);
    }
    list_free_deep(distkeyInfo);
#endif
}

static void check_table(RangeTblEntry *rte, Oid *mv_groupid) {
    if (rte->rtekind == RTE_SUBQUERY) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("complicated subquery is not supported, except UNION ALL")));
    }
    if ((rte->rtekind != RTE_RELATION) || (rte->relkind != RELKIND_RELATION)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("relation \"%s\" is not a regular table", rte->relname)));
    }
    if (IsSystemObjOid(rte->relid)) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("relation \"%s\" is a system relation", rte->relname)));
    }
    if (rte->relhasbucket) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("relation \"%s\" has bucket", rte->relname)));
    }
    if (rte->orientation != REL_ROW_ORIENTED) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("relation \"%s\" is not row oriented", rte->relname)));
    }
    if (GetLocatorType(rte->relid) == LOCATOR_TYPE_REPLICATED) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("relation \"%s\" is distributed by replication", rte->relname)));
    }
    if (rte->ispartrel) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("It is not supported to create incremental materialized view on partitioned table \"%s\"", rte->relname)));
    }

    Oid rel_groupid = ng_get_baserel_groupoid(rte->relid, RELKIND_RELATION);
    if (rel_groupid != *mv_groupid && *mv_groupid != InvalidOid) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Not support matview and baserel are not in same node group. ")));
    }
    *mv_groupid = rel_groupid;

    return;
}

/* When create or refresh matview, we need check base table is whether supported. */
void check_basetable(Query *query, bool isCreateMatview, bool isIncremental)
{
    List* rteList = NIL;
    (void) query_tree_walker(query,
                             (bool (*)())BasetableWalker,
                             (void*)&rteList,
                             QTW_EXAMINE_RTES | QTW_IGNORE_DUMMY);

    ListCell* lc = NULL;
    foreach (lc, rteList) {
        RangeTblEntry *rte = (RangeTblEntry*)lfirst(lc);

        /* only check nodegroup for create matview */
        if (isCreateMatview) {
#ifdef ENABLE_MULTIPLE_NODES
            if (!isIncremental && (get_pgxc_class_groupoid(rte->relid) != ng_get_installation_group_oid())) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("relation \"%s\" is distributed in a non-installation node group, which is not supported",
                        rte->relname)));
            }
#endif

            if (rte->is_ustore) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("materialized view is not supported in ustore yet")));
            }

            Relation rel = heap_open(rte->relid, AccessShareLock);
            if (RelationisEncryptEnable(rel)) {
                ereport(ERROR, (errmodule(MOD_SEC_TDE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("create matview on TDE table failed"),
                            errdetail("materialized views do not support TDE feature"),
                                errcause("create materialized views is not supported on TDE table"),
                                    erraction("check CREATE syntax about create the materialized views")));
            }
            if (RelationUsesLocalBuffers(rel)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("materialized views must not use temporary tables or views")));
            }

            if (isIncremental && (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)) {
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Can not create incremental materialized view on unlogged table")));
            }

            heap_close(rel, AccessShareLock);
        }

        /* privileges are both checked when create/refresh matview */
        (void)CheckPermissionForBasetable(rte);
    }

    list_free_ext(rteList);
}

static bool BasetableWalker(Node *node, List** rteList) {
    if (node == NULL) {
        return false;
    }
    if (IsA(node, RangeTblEntry)) {
        RangeTblEntry *rte = (RangeTblEntry*)node;
        if ((rte->rtekind != RTE_RELATION) || (rte->relid == InvalidOid)) {
            return false;
        }
        *rteList = lappend(*rteList, rte);
        return false;
    }

    if (IsA(node, Query)) {
        /* Recurse into subselects */
        return query_tree_walker((Query*)node,
                                 (bool (*)())BasetableWalker,
                                 (void*)rteList,
                                 QTW_EXAMINE_RTES | QTW_IGNORE_DUMMY);
    }

    return expression_tree_walker(node, (bool (*)())BasetableWalker, (void*)rteList);
}

static List* get_distkey_info(Oid oid) {
    Relation rel;
    ScanKeyData scanKey;
    SysScanDesc scan;
    HeapTuple tup;
    Form_pgxc_class pgxc_class;
    List *distkey_infos = NULL;
    DistKeyInfo *info = NULL;

    ScanKeyInit(&scanKey, Anum_pgxc_class_pcrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    rel = heap_open(PgxcClassRelationId, AccessShareLock);
    scan = systable_beginscan(rel, PgxcClassPgxcRelIdIndexId, true, NULL, 1, &scanKey);
    tup = systable_getnext(scan);

    Assert(HeapTupleIsValid(tup));

    pgxc_class = (Form_pgxc_class)GETSTRUCT(tup);

    if ((pgxc_class->pchashalgorithm != 1) || (pgxc_class->pcattnum.values[0] == InvalidAttrNumber)) {
        Relation relation = heap_open(oid, NoLock);
        char *relname = pstrdup(RelationGetRelationName(relation));
        heap_close(relation, NoLock);
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("relation \"%s\" is not distributed by hash", relname)));
    }

    for (int i = 0; i < pgxc_class->pcattnum.dim1; i++) {
        info = (DistKeyInfo *) palloc0(sizeof(DistKeyInfo));
        info->attnum = pgxc_class->pcattnum.values[i];
        info->atttype = get_atttype(oid, info->attnum);
        info->atttypmod = get_atttypmod(oid, info->attnum);
        distkey_infos = lappend(distkey_infos, info);
    }

    systable_endscan(scan);
    heap_close(rel, AccessShareLock);

    return distkey_infos;
}

void create_matview_meta(Query *query, RangeVar *rel, bool incremental)
{
    Oid mapid = InvalidOid;
    Oid matviewid;

    matviewid = RangeVarGetRelid(rel, NoLock, false);

    if (incremental) {
        mapid = create_matview_map(matviewid);

        ListCell *lc = NULL;
        List *relids = pull_up_rels_recursive((Node *)query);
        foreach(lc, relids) {
            Oid relid = (Oid)lfirst_oid(lc);
            Oid mlogid = find_mlog_table(relid);
            insert_matviewdep_tuple(matviewid, relid, mlogid);
        }
    }

    create_matview_tuple(matviewid, mapid, incremental);

    return;
}

DistributeBy *infer_incmatview_distkey(CreateTableAsStmt *stmt) {
    Query *query = NULL;
    RangeTblEntry *rte = NULL;
    ListCell *ic = NULL;
    ListCell *tc = NULL;
    ListCell *lc = NULL;
    List *distkeyInfo = NULL;
    List *distColNames = NULL;
    DistributeBy *distributeby = NULL;

    query = (Query*)stmt->query;

    if (query->setOperations != NULL) {
        rte = (RangeTblEntry *)linitial(query->rtable);
        query = rte->subquery;
    }
    if (query->rtable == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Cannot infer distribute key from the given query")));
    }

    rte = (RangeTblEntry*)linitial(query->rtable);
    Assert(rte->relname != NULL);
    distkeyInfo = get_distkey_info(rte->relid);
    Assert(distkeyInfo != NULL);

    foreach (ic, distkeyInfo) {
        DistKeyInfo *dkInfo = (DistKeyInfo *)lfirst(ic);
        // intoClause.colNames is manually traversed in case it might be NULL
        // forboth can not be used here
        lc = list_head(stmt->into->colNames);
        foreach (tc, query->targetList) {
            TargetEntry *te = (TargetEntry *)lfirst(tc);
            if (IsA(te->expr, Var) && (te->resorigcol == dkInfo->attnum)) {
                Value *givenName = lc ? (Value*)lfirst(lc) : makeString(pstrdup(te->resname));
                distColNames = lappend(distColNames, givenName);
                break;
            }
            lc = lc ? lnext(lc) : NULL;
        }
    }
    distributeby = makeNode(DistributeBy);
    distributeby->disttype = DISTTYPE_HASH;
    distributeby->colname = distColNames;
    return distributeby;
}

/*
 * clear-up matviewmap table
 */
static void clearup_matviewmap_tuple(Oid mapid)
{
    TableScanDesc scan;
    HeapTuple tuple;

    if (!OidIsValid(mapid)) {
        return;
    }

    Relation maprel = heap_open(mapid, RowExclusiveLock);

    scan = tableam_scan_begin(maprel, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        simple_heap_delete(maprel, &tuple->t_self);
    }
    tableam_scan_end(scan);

    heap_close(maprel, RowExclusiveLock);
}

static void vacuum_mlog_for_matview(Oid matviewid) {
    Relation rel = NULL;
    TableScanDesc scan;
    ScanKeyData scanKey;
    HeapTuple tup = NULL;
    Form_gs_matview_dependency dep;
    List *mlogidList = NULL;
    ListCell *mlogidCell = NULL;

    ScanKeyInit(&scanKey,
            Anum_gs_matview_dep_matviewid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(matviewid));
    rel = heap_open(MatviewDependencyId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 1, &scanKey);

    while ((tup = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL ) {
        dep = (Form_gs_matview_dependency)GETSTRUCT(tup);
        mlogidList = lappend_oid(mlogidList, dep->mlogid);
    }
    tableam_scan_end(scan);
    heap_close(rel, NoLock);

    foreach (mlogidCell, mlogidList) {
        vacuum_mlog(lfirst_oid(mlogidCell));
    }
    list_free(mlogidList);
}

static void vacuum_mlog(Oid mlogid) {
    Relation depRel = NULL;
    TableScanDesc depScan;
    ScanKeyData scanKey;
    HeapTuple tup = NULL;
    Form_gs_matview_dependency dep;
    List *matviewidList = NULL;
    ListCell *matviewidCell = NULL;
    Timestamp minRefreshTime = DT_NOEND;
    bool isNull = false;

    // 1. find all the matviews relavant to this mlog
    ScanKeyInit(&scanKey,
            Anum_gs_matview_dep_mlogid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            ObjectIdGetDatum(mlogid));
    depRel = heap_open(MatviewDependencyId, AccessShareLock);
    depScan = tableam_scan_begin(depRel, SnapshotNow, 1, &scanKey);

    while ((tup = (HeapTuple) tableam_scan_getnexttuple(depScan, ForwardScanDirection)) != NULL ) {
        dep = (Form_gs_matview_dependency)GETSTRUCT(tup);
        matviewidList = lappend_oid(matviewidList, dep->matviewid);
    }
    tableam_scan_end(depScan);
    heap_close(depRel, NoLock);

    // 2. determine the min refresh time 'rtmin' of relavant matviews
    foreach (matviewidCell, matviewidList) {
        Oid matviewid = lfirst_oid(matviewidCell);
        Datum refreshTimeDatum = get_matview_refreshtime(matviewid, &isNull);
        Timestamp refreshTime = DatumGetTimestamp(refreshTimeDatum);
        if (isNull) {
            list_free(matviewidList);
            return;
        }
        if (timestamp_cmp_internal(refreshTime, minRefreshTime) < 0) {
            minRefreshTime = refreshTime;
        }
    }
    list_free(matviewidList);

    // 3. remove the tuples with refresh time earlier than 'rtmin'
    if (!TIMESTAMP_IS_NOEND(minRefreshTime)) {
        MlogClearLogByTime(mlogid, minRefreshTime);
    }
}

static void MlogClearLogByTime(Oid mlogID, Timestamp ts)
{
    TableScanDesc scan;
    bool isNull = true;
    HeapTuple tuple;
    ScanKeyData entry[1];

    ScanKeyInit(&entry[0],
                MlogAttributeTime,
                BTLessStrategyNumber,
                F_TIMESTAMP_LT,
                TimestampGetDatum(ts));

    Relation rel = heap_open(mlogID, RowExclusiveLock); //TODO: high lock

    scan = tableam_scan_begin(rel, GetTransactionSnapshot(), 1, entry);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        (void)heap_getattr(tuple,
                           MlogAttributeTime,
                           RelationGetDescr(rel),
                           &isNull);

        if (isNull) {
            continue;
        }
        simple_heap_delete(rel, &tuple->t_self);
    }

    tableam_scan_end(scan);
    heap_close(rel, NoLock);

    return;
}
